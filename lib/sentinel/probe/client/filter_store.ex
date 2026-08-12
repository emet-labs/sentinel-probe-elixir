defmodule Sentinel.Probe.SDK.Client.FilterStore do
  @moduledoc """
  Holds the current `EventFilter` for the Probe's source and tracks the
  acknowledged epoch. Elixir analog of `sdk/go/client/filterstore.go`
  (`filter-store.ts`).

  ## `:persistent_term`, not a GenServer

  Reads are lock-free and stay on the hot emit/enforce path; writes are rare
  (one per filter push, per epoch). `:persistent_term.get/1` is a single cheap
  Erlang term lookup with no message passing, so a Probe that emits from many
  processes while a filter push arrives on another sees a consistent snapshot
  without a GenServer bottleneck. `:persistent_term.put/2` broadcasts to all
  schedulers — acceptable for a per-epoch write, wrong for a per-event one.

  ## Epoch compared BY VALUE

  The held filter is an immutable struct, so there is no pointer-aliasing trap:
  two distinct allocations holding the same epoch compare equal under `==`,
  because Elixir integers are values. This is the property the Go analog has to
  spell out with `equalEpoch` (Go compares `*uint64` pointers, which would make
  Set report "updated" on every push); in Elixir it is free.

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

  @doc """
  Builds a store bound to `key` and seeds it with `initial` (or `nil`).

  `key` MUST be unique per Probe (per `source_handle`), since
  `:persistent_term` is process-global. A caller that restores a filter from a
  local cache before the first push passes it as `initial`.
  """
  @spec new(key(), EventFilter.t() | nil) :: t()
  def new(key, initial \\ nil) do
    :persistent_term.put(key, initial)
    %__MODULE__{key: key}
  end

  @doc """
  Returns the held `EventFilter`, or `nil` before the first `set/2`.

  The returned struct is immutable and stable until the next `set/2`: it is safe
  to hold it across an entire emit-and-enforce flow. Lock-free: a single
  `:persistent_term.get/1`, no message passing.
  """
  @spec get(t()) :: EventFilter.t() | nil
  def get(%__MODULE__{key: key}), do: :persistent_term.get(key)

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
  Swaps in a new filter. Returns whether the store was actually updated, which is
  `true` when the epoch changed or when this is the first `set/2`, and `false`
  when an equal epoch is re-pushed.

  Mirrors `filter-store.ts:31-38`, including the subtle case where the held
  filter and the new filter both carry no epoch: that still counts as an update
  on the first `set/2`, because the "not first set" conjunct is what makes the
  no-op branch reachable.
  """
  @spec set(t(), EventFilter.t()) :: boolean()
  def set(%__MODULE__{key: key} = store, %EventFilter{} = filter) do
    held = get(store)

    if held != nil and held.epoch == filter.epoch do
      false
    else
      :persistent_term.put(key, filter)
      true
    end
  end

  @doc """
  Reports whether an announced epoch differs from the held one.

  No filter held means refresh; no announced epoch (`nil`) means there is nothing
  to compare against, so do not refresh.
  """
  @spec should_refresh(t(), non_neg_integer() | nil) :: boolean()
  def should_refresh(%__MODULE__{}, nil), do: false

  def should_refresh(%__MODULE__{} = store, new_epoch) do
    case epoch(store) do
      nil -> true
      held -> new_epoch != held
    end
  end

  @doc """
  Removes the held term from `:persistent_term`. Call on shutdown or in tests to
  keep the global table clean.
  """
  @spec delete(t()) :: :ok
  def delete(%__MODULE__{key: key}) do
    :persistent_term.erase(key)
    :ok
  end
end
