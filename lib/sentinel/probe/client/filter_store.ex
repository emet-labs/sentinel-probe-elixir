defmodule Sentinel.Probe.SDK.Client.FilterStore do
  @moduledoc """
  Holds the current `EventFilter` for the Probe's source and tracks the
  acknowledged epoch. Elixir analog of `sdk/go/client/filterstore.go`
  (`filter-store.ts`).

  ## Reads from `:persistent_term`, writes through one writer process

  Reads are lock-free and stay on the hot emit/enforce path; writes are rare
  (one per filter push, per epoch). `:persistent_term.get/1` is a single cheap
  Erlang term lookup with no message passing, so a Probe that emits from many
  processes while a filter push arrives on another sees a consistent snapshot
  without a GenServer bottleneck. `:persistent_term.put/2` broadcasts to all
  schedulers — acceptable for a per-epoch write, wrong for a per-event one.

  Every write goes through one writer process per store, which applies sets in
  the order its mailbox receives them. The epoch read-compare-write is
  therefore serialized locally, with no lock, no retry and no back-off sleep: a
  set waits only for the sets queued ahead of it. `set/2` waits for its own
  result; `set_async/2` returns at once, so the OTLP log receiver never blocks
  decision traffic on a Filter apply. Writers run under the SDK application's
  supervisor and are named by store key, so there is at most one per key; one
  that dies is reported and started again on next use.

  ## Epoch compared BY VALUE

  The held filter is an immutable struct, so there is no pointer-aliasing trap:
  two distinct allocations holding the same epoch compare equal under `==`,
  because Elixir integers are values. This is the property the Go analog has to
  spell out with `replaces` (Go compares `*uint64` pointers); in Elixir it is
  free.

  ## The epoch-0 trap

  `filter.epoch` is a proto3 optional. It is `nil` when the filter declares no
  epoch and an integer (including `0`) when it does. `nil` means "absent"; it
  does NOT mean zero: epoch 0 is a legitimate epoch and must not be classified
  as "no policy held". `is_nil(filter.epoch)` is the presence test.
  """

  alias Sentinel.Model.V1.EventFilter

  defstruct [:key]

  @type key :: term()
  @type t :: %__MODULE__{key: key()}

  defmodule Writer do
    @moduledoc false
    # Serializes every write to a store. Each set is one message, handled in
    # mailbox order; nothing here sleeps or retries. Supervised under
    # FilterStore.Supervisor and locally registered under a name derived from
    # the store key, so at most one writer runs per name. Local registration is
    # released the instant the process exits, so a lookup never returns a dead
    # writer. A writer that dies is reported by its supervisor and started again
    # by the next caller that needs it (see `FilterStore.writer/1`); its state
    # lives in :persistent_term, so nothing but its unprocessed mailbox is lost.
    # The key travels in each message, so two keys whose names collide merely
    # share serialization.
    use GenServer, restart: :temporary

    def start_link(name), do: GenServer.start_link(__MODULE__, nil, name: name)

    @impl true
    def init(nil), do: {:ok, nil}

    @impl true
    def handle_call({:set, key, filter}, _from, state), do: {:reply, swap(key, filter), state}

    # Delete runs here, in line with the swaps for the key: every set queued
    # before it applies, then the tombstone is written and the Filter erased,
    # and every later swap (in this writer or a successor) sees the tombstone.
    def handle_call({:delete, key}, _from, state) do
      :persistent_term.put(Sentinel.Probe.SDK.Client.FilterStore.tombstone(key), true)
      :persistent_term.erase(key)
      {:reply, :ok, state}
    end

    @impl true
    def handle_cast({:set, key, filter}, state) do
      swap(key, filter)
      {:noreply, state}
    end

    defp swap(key, filter) do
      held = :persistent_term.get(key, nil)

      cond do
        Sentinel.Probe.SDK.Client.FilterStore.deleted_key?(key) ->
          false

        held == nil or replaces?(held.epoch, filter.epoch) ->
          :persistent_term.put(key, filter)
          true

        true ->
          false
      end
    end

    # An absent offered epoch never replaces; any offered epoch replaces an
    # absent held one; otherwise only a strictly greater epoch does. Elixir
    # integers are unbounded, so the unsigned 64-bit order is plain `>`.
    defp replaces?(_held, nil), do: false
    defp replaces?(nil, _offered), do: true
    defp replaces?(held, offered), do: offered > held
  end

  @doc """
  Builds a store bound to `key` and seeds it with `initial` (or `nil`).

  `key` MUST be unique per Probe (per `source_handle`), since
  `:persistent_term` is process-global. A caller that restores a filter from a
  local cache before the first push passes it as `initial`. The store's writer
  is registered under a name derived from `key`, so concurrent `new/2` calls
  for one key share one writer and writes to that key stay serialized. The
  writer lives until `delete/1`. A `new/2` after `delete/1` brings the key back.
  """
  @spec new(key(), EventFilter.t() | nil) :: t()
  def new(key, initial \\ nil) do
    :persistent_term.erase(tombstone(key))
    :persistent_term.put(key, initial)
    store = %__MODULE__{key: key}
    _ = writer(store)
    store
  end

  @doc """
  Returns the pid of the store's writer, starting it when none is running.

  Starting is atomic per key: the writer registers under a name derived from
  the key, and a racing start returns the writer that won. A writer that has
  died is started again here, so the next `set/2` or `set_async/2` after a
  crash reaches a live one. Exposed for receivers that monitor the writer and
  for diagnostics.
  """
  @spec writer(t()) :: pid()
  def writer(%__MODULE__{key: key}) do
    name = writer_name(key)

    case Process.whereis(name) do
      nil -> start_writer(name)
      pid -> pid
    end
  end

  @doc false
  # The running writer for the store, or nil. Never starts one.
  @spec running_writer(t()) :: pid() | nil
  def running_writer(%__MODULE__{key: key}), do: Process.whereis(writer_name(key))

  @doc """
  Reports whether `delete/1` has removed the store. A deleted store holds no
  Filter, refuses every set, and starts no writer, until `new/2` recreates it.
  """
  @spec deleted?(t()) :: boolean()
  def deleted?(%__MODULE__{key: key}), do: deleted_key?(key)

  @doc false
  def deleted_key?(key), do: :persistent_term.get(tombstone(key), false)

  @doc false
  def tombstone(key), do: {__MODULE__, :deleted, key}

  # One atom per distinct store key; a Probe holds one store per source.
  defp writer_name(key) do
    String.to_atom("#{__MODULE__}.Writer.#{:erlang.phash2(key, 4_294_967_296)}")
  end

  defp start_writer(name) do
    case DynamicSupervisor.start_child(__MODULE__.Supervisor, {Writer, name}) do
      {:ok, pid} -> pid
      {:error, {:already_started, pid}} -> pid
    end
  end

  @doc """
  Returns the held `EventFilter`, or `nil` before the first `set/2`.

  The returned struct is immutable and stable until the next `set/2`: it is safe
  to hold it across an entire emit-and-enforce flow. Lock-free: a single
  `:persistent_term.get/1`, no message passing.
  """
  @spec get(t()) :: EventFilter.t() | nil
  def get(%__MODULE__{key: key}), do: :persistent_term.get(key, nil)

  @doc """
  Returns the held filter's epoch, or `nil` when no filter is held or the held
  filter declares no epoch.

  `nil` means "absent". It does not mean zero: epoch 0 is a legitimate epoch and
  is returned as `0`, not `nil`.
  """
  @spec epoch(t()) :: non_neg_integer() | nil
  def epoch(%__MODULE__{} = store) do
    case get(store) do
      nil -> nil
      %EventFilter{epoch: epoch} -> epoch
    end
  end

  @doc """
  Swaps in a new filter under the epoch order. Returns whether the store was
  actually updated.

  Epochs are unsigned and only move forward (ADR-0006). With *held* the held
  filter's epoch and *new* the offered one's, either of which may be absent:

    * Nothing held: any filter applies, including epoch 0 and an epoch-less one.
    * Held is absent: a filter with an epoch (including 0) replaces it; an
      epoch-less one is a no-op.
    * Held is present: only a strictly greater epoch replaces it. An equal
      epoch is a no-op, the ordinary at-least-once re-delivery. A lower or
      absent epoch is refused, so a reordered or replayed push never
      downgrades the filter or the acknowledged epoch.

  A `set/2` that returns `false` keeps the held filter and changes no later
  comparison; it raises nothing and logs nothing.

  An equal epoch is never applied, even when its content differs. The server
  rewrites the current epoch in place when its Readiness flips Warming to
  Active, so whether a Probe should accept that flip at an equal epoch is an
  open question (#534); the store keeps its earlier behaviour until it is
  decided. After a server store reset, epochs restart from 1 and the store
  refuses them until the Probe restarts; restarting is the recovery.

  Mirrors `filter-store.ts`. The read-compare-write runs in the store's writer
  process, so concurrent sets from different processes are applied one at a
  time in arrival order and can never leave the lower epoch held. Reads stay
  lock-free.

  If the writer dies while this call is queued, the call is retried once on a
  freshly started writer; a set is idempotent under the epoch order, so the
  retry cannot apply anything twice. A second failure exits the caller. A
  deleted store refuses the set and starts no writer.
  """
  @spec set(t(), EventFilter.t()) :: boolean()
  def set(%__MODULE__{key: key} = store, %EventFilter{} = filter) do
    if deleted?(store), do: false, else: call_writer(store, {:set, key, filter})
  end

  defp call_writer(store, message) do
    GenServer.call(writer(store), message, :infinity)
  catch
    :exit, _reason -> GenServer.call(writer(store), message, :infinity)
  end

  @doc """
  Hands `filter` to the store's writer under the same epoch order as `set/2`
  and returns at once. Filters sent by one process are applied in the order it
  sent them. The OTLP log receiver uses this so a Filter apply never blocks
  decision registration or delivery.

  A writer that is not running is started first. Filters still queued in a
  writer when it dies are lost with its mailbox, so a caller that must not lose
  them monitors `writer/1` and resends on `:DOWN`, as the OTLP log receiver
  does. A deleted store drops the Filter and starts no writer.
  """
  @spec set_async(t(), EventFilter.t()) :: :ok
  def set_async(%__MODULE__{key: key} = store, %EventFilter{} = filter) do
    if deleted?(store), do: :ok, else: GenServer.cast(writer(store), {:set, key, filter})
  end

  @doc """
  Reports whether an announced epoch is strictly newer than the held one.

  No epoch held means refresh; no announced epoch (`nil`) means there is
  nothing to compare against, so do not refresh.
  """
  @spec should_refresh(t(), non_neg_integer() | nil) :: boolean()
  def should_refresh(%__MODULE__{} = store, new_epoch) do
    case epoch(store) do
      nil -> true
      held -> new_epoch != nil and new_epoch > held
    end
  end

  @doc """
  Removes the held Filter from `:persistent_term` and stops the store's writer.
  Call on shutdown or in tests to keep the global table clean.

  The delete is applied by the writer, after every set already queued for the
  key, and leaves a tombstone: from then on the store holds nothing, refuses
  every set (`set/2`, `set_async/2`, and any set still in flight) and starts no
  writer, so an OTLP log receiver still attached to it cannot bring it back
  with a later push or a replay. `new/2` for the key clears the tombstone.
  """
  @spec delete(t()) :: :ok
  def delete(%__MODULE__{key: key} = store) do
    :ok = call_writer(store, {:delete, key})

    case running_writer(store) do
      nil -> :ok
      pid -> DynamicSupervisor.terminate_child(__MODULE__.Supervisor, pid)
    end

    :ok
  end
end
