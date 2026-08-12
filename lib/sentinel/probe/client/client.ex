defmodule Sentinel.Probe.SDK.Client.Client do
  @moduledoc """
  Owns the Probe's filter state and builds decision requests against it.
  Elixir analog of `sdk/go/client/client.go` (`probe-client.ts`).

  All mutable state lives behind `FilterStore`'s `:persistent_term` term, so a
  Probe that emits from many processes while a filter push arrives on another
  sees a consistent snapshot.
  """

  alias Sentinel.Model.V1.{EventFilter, ProducerEvent}
  alias Sentinel.Probe.V1.DecideRequest
  alias Sentinel.Probe.SDK.Client.{FilterStore, Transport}

  defstruct [:config, :store, :decider]

  @type decide_fn :: (DecideRequest.t() -> {:ok, term()} | {:error, term()})

  @type config :: %__MODULE__.Config{
          source_handle: String.t(),
          sentinel_base_url: String.t(),
          initial_filter: EventFilter.t() | nil
        }

  defmodule Config do
    @moduledoc false
    defstruct [:source_handle, :sentinel_base_url, :initial_filter]
  end

  @type t :: %__MODULE__{
          config: config(),
          store: FilterStore.t(),
          decider: decide_fn()
        }

  @doc """
  Builds a `ProbeClient`.

  `decider` is REQUIRED with no default: a silently-defaulted transport hides a
  misconfigured endpoint until the first enforcement call. In production build it
  with `transport_decider/2`; in tests pass a stub `fn _req -> {:ok, resp} end`.
  """
  @spec new(config(), decide_fn()) :: t()
  def new(%Config{source_handle: source_handle, initial_filter: initial} = config, decider) do
    key = {:sentinel_probe_filter, source_handle}
    store = FilterStore.new(key, initial)
    %__MODULE__{config: config, store: store, decider: decider}
  end

  @doc "Returns the held `EventFilter`, or `nil` before the first `set_filter/2`."
  @spec current_filter(t()) :: EventFilter.t() | nil
  def current_filter(%__MODULE__{store: store}), do: FilterStore.get(store)

  @doc """
  Returns the held filter epoch, the value a Probe stamps into
  `ProducerEvent.acknowledged_filter_epoch`. `nil` means no epoch is held; it
  does not mean 0.
  """
  @spec acknowledged_epoch(t()) :: non_neg_integer() | nil
  def acknowledged_epoch(%__MODULE__{store: store}), do: FilterStore.epoch(store)

  @doc """
  Swaps in a new `EventFilter`, reporting whether the store was actually updated.

  Where the new filter comes from is out of SDK scope: in v1 Sentinel pushes
  filters (ADR-0006) and no push RPC exists in `decision.proto`, so the host
  calls `set_filter/2` when a push arrives. Same division of labour as the
  reference.
  """
  @spec set_filter(t(), EventFilter.t()) :: boolean()
  def set_filter(%__MODULE__{store: store}, filter), do: FilterStore.set(store, filter)

  @doc "Reports whether an announced epoch warrants fetching a new filter."
  @spec refresh_on_epoch(t(), non_neg_integer() | nil) :: boolean()
  def refresh_on_epoch(%__MODULE__{store: store}, new_epoch),
    do: FilterStore.should_refresh(store, new_epoch)

  @doc """
  Builds a `DecideRequest` against the held filter epoch.

  The event must already have been projected by `Sentinel.Probe.SDK.Filter`; this
  method does not project, exactly as the reference does not. `remaining_budget_ns`
  is `nil` when the caller set no latency budget.
  """
  @spec build_decide_request(
          t(),
          ProducerEvent.t(),
          String.t(),
          String.t(),
          non_neg_integer() | nil
        ) ::
          DecideRequest.t()
  def build_decide_request(
        %__MODULE__{config: %Config{source_handle: source_handle}, store: store},
        event,
        request_id,
        idempotency_key,
        remaining_budget_ns
      ) do
    %DecideRequest{
      request_id: request_id,
      idempotency_key: idempotency_key,
      source_handle: source_handle,
      filter_epoch: FilterStore.epoch(store),
      producer_event: event,
      remaining_transport_budget_nanoseconds: remaining_budget_ns
    }
  end

  @doc "The configured source handle, stamped into every `DecideRequest`."
  @spec source_handle(t()) :: String.t()
  def source_handle(%__MODULE__{config: %Config{source_handle: source_handle}}), do: source_handle

  @doc """
  Exposes the decider as the plain function the enforcement gate's `:decide`
  dependency expects.

  The enforcement package deliberately takes a plain function rather than a
  transport, so it stays testable without one; this is the adapter between the
  two. The Go analog wraps a Connect client and unwraps its envelope; on BEAM
  the decider is already a plain `fn request -> {:ok, resp} | {:error, term}`,
  so this is a direct accessor.
  """
  @spec decide_func(t()) :: decide_fn()
  def decide_func(%__MODULE__{decider: decider}), do: decider

  @doc """
  Builds a decider function backed by the Connect `application/proto` transport.

  Convenience so a host does not write the wrapper by hand:

      decider = Client.transport_decider(config.sentinel_base_url, :my_finch)
      probe = Client.new(config, decider)

  `opts` are passed through to `Transport.decide/4` (e.g. `receive_timeout`).
  """
  @spec transport_decider(String.t(), atom(), keyword()) :: decide_fn()
  def transport_decider(base_url, finch, opts \\ []) do
    fn request -> Transport.decide(base_url, request, finch, opts) end
  end
end
