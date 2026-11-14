defmodule Sentinel.Probe.SDK.Emission.SequenceError do
  @moduledoc """
  A Sequence allocator or completeness heartbeat refusal with a stable `code`
  shared with every Probe SDK and the `sequence_completeness` vectors:

    * `"epoch-not-greater"` — an explicit epoch not strictly greater than the
      previous run's epoch;
    * `"sequence-overflow"` — an epoch or a number past 2^63 - 1;
    * `"filter-epoch-regressed"` — the acknowledged Filter epoch went below one
      already sent for the epoch, so the heartbeat sent nothing.
  """

  defexception [:code, :message]

  @type t :: %__MODULE__{code: String.t(), message: String.t()}

  @doc false
  def new(code, message), do: %__MODULE__{code: code, message: message}
end

defmodule Sentinel.Probe.SDK.Emission.Sequence do
  @moduledoc """
  Numbers a source's Events and tracks which numbers Sentinel holds (#461).

  One allocator serves one source. `allocate/1` hands out
  `%{epoch: e, number: n}` slots, and the exporter marks each slot exported,
  failed or filtered. The completeness heartbeat reads the declaration:
  `last_emitted` is the highest number allocated, and `complete_through` is the
  contiguous prefix whose export succeeded. `complete_through` never passes a
  slot that is in flight, failed or filtered, so missing evidence is never
  declared complete.

  A declared close (`complete_through == last_emitted`, returned by
  `declare/1`) seals the epoch: Sentinel refuses any later slot in it. The next
  allocation therefore opens a new epoch, `max(epoch + 1, clock ms)`. The roll
  is lazy, at allocation and never at the heartbeat, so a quiet source keeps its
  epoch.

  ## A GenServer, not `:atomics`

  Sealing against allocation is a compound transition (read the counters, seal,
  or roll and allocate), which one counter cell cannot express. The GenServer
  serialises it: an allocation either lands before a `declare/1`, which then
  sees it in flight and does not seal, or after it, and opens a new epoch.

  ## Options

    * `:now` — `(-> integer)` Unix nanoseconds; the epoch is its milliseconds.
      Defaults to `System.os_time(:nanosecond)`. Tests inject it.
    * `:epoch` — overrides the clock-derived first epoch.
    * `:previous_epoch` — the last epoch of the previous run, when the host
      persisted it. Without it a backward clock step across a restart goes
      undetected, and Sentinel refuses the regressed epoch.
    * `:name` — a GenServer name.

  Epochs and numbers are integers in `0..2^63 - 1`. Past the maximum is a
  `SequenceError` with code `"sequence-overflow"`, never a wrap.
  """

  use GenServer

  alias Sentinel.Probe.SDK.Emission.SequenceError

  @int64_max 9_223_372_036_854_775_807
  @nanos_per_ms 1_000_000

  @type slot :: %{epoch: non_neg_integer(), number: non_neg_integer()}
  @type filtered_range :: %{
          first: non_neg_integer(),
          last: non_neg_integer(),
          filter_epoch: non_neg_integer()
        }
  @type epoch_declaration :: %{
          epoch: non_neg_integer(),
          last_emitted: non_neg_integer(),
          complete_through: non_neg_integer()
        }
  @type declaration :: %{
          epoch: non_neg_integer(),
          last_emitted: non_neg_integer(),
          complete_through: non_neg_integer(),
          filtered: [filtered_range()],
          previous: epoch_declaration() | nil
        }

  @doc """
  Starts an allocator. Returns `{:error, %SequenceError{}}` when the explicit
  epoch is not greater than `:previous_epoch`, or when `:previous_epoch` has no
  successor. Raises `ArgumentError` for an epoch outside `0..2^63 - 1`.
  """
  @spec start_link(keyword()) :: GenServer.on_start() | {:error, SequenceError.t()}
  def start_link(opts \\ []) do
    now = Keyword.get(opts, :now, fn -> System.os_time(:nanosecond) end)
    epoch = opts |> Keyword.get(:epoch) |> checked(:epoch)
    previous = opts |> Keyword.get(:previous_epoch) |> checked(:previous_epoch)

    with {:ok, first} <- first_epoch(epoch, previous, now) do
      GenServer.start_link(__MODULE__, {first, now}, Keyword.take(opts, [:name]))
    end
  end

  @doc "Allocates the next slot. After a declared close it first opens a new epoch."
  @spec allocate(GenServer.server()) :: {:ok, slot()} | {:error, SequenceError.t()}
  def allocate(server), do: GenServer.call(server, :allocate)

  @doc """
  Marks a slot exported: Sentinel accepted its Event (HTTP 2xx, zero rejected
  spans). A failed slot the host re-exported may be marked exported.
  """
  @spec mark_exported(GenServer.server(), slot()) :: :ok
  def mark_exported(server, %{epoch: _, number: _} = slot),
    do: GenServer.call(server, {:mark_exported, slot})

  @doc """
  Marks a slot failed. The helper does not retry it; the epoch stays open below
  the slot until the host re-exports the identical Event.
  """
  @spec mark_failed(GenServer.server(), slot()) :: :ok
  def mark_failed(server, %{epoch: _, number: _} = slot),
    do: GenServer.call(server, {:mark_failed, slot})

  @doc """
  Marks a slot dropped by the Event Filter under `filter_epoch`. Until #542 a
  filtered slot does not advance `complete_through`.
  """
  @spec mark_filtered(GenServer.server(), slot(), non_neg_integer()) :: :ok
  def mark_filtered(server, %{epoch: _, number: _} = slot, filter_epoch)
      when is_integer(filter_epoch) and filter_epoch >= 0,
      do: GenServer.call(server, {:mark_filtered, slot, filter_epoch})

  @doc "The current Sequence Epoch."
  @spec epoch(GenServer.server()) :: non_neg_integer()
  def epoch(server), do: GenServer.call(server, :epoch)

  @doc "The highest number allocated in the current epoch, `0` before any."
  @spec highest_allocated(GenServer.server()) :: non_neg_integer()
  def highest_allocated(server), do: GenServer.call(server, :highest_allocated)

  @doc "Reads the current declaration without sealing anything."
  @spec declaration(GenServer.server()) :: declaration()
  def declaration(server), do: GenServer.call(server, :declaration)

  @doc """
  Returns the declaration the heartbeat sends. A close (`complete_through ==
  last_emitted`) seals the epoch, so no slot is ever allocated in it afterwards.
  """
  @spec declare(GenServer.server()) :: declaration()
  def declare(server), do: GenServer.call(server, :declare)

  @impl true
  def init({epoch, now}) do
    {:ok,
     %{
       now: now,
       epoch: epoch,
       last: 0,
       complete_through: 0,
       sealed: false,
       states: %{},
       filtered: [],
       previous: nil
     }}
  end

  @impl true
  def handle_call(:allocate, _from, state) do
    with {:ok, state} <- roll(state),
         {:ok, number} <- successor(state.last) do
      state = %{state | last: number, states: Map.put(state.states, number, :in_flight)}
      {:reply, {:ok, %{epoch: state.epoch, number: number}}, state}
    else
      {:error, error} -> {:reply, {:error, error}, state}
    end
  end

  def handle_call({:mark_exported, slot}, _from, state) do
    case state_of(state, slot) do
      status when status in [:in_flight, :failed] ->
        state = %{state | states: Map.put(state.states, slot.number, :exported)}
        {:reply, :ok, advance(state)}

      _ ->
        {:reply, :ok, state}
    end
  end

  def handle_call({:mark_failed, slot}, _from, state) do
    case state_of(state, slot) do
      :in_flight -> {:reply, :ok, %{state | states: Map.put(state.states, slot.number, :failed)}}
      _ -> {:reply, :ok, state}
    end
  end

  def handle_call({:mark_filtered, slot, filter_epoch}, _from, state) do
    case state_of(state, slot) do
      :in_flight ->
        range = %{first: slot.number, last: slot.number, filter_epoch: filter_epoch}

        state = %{
          state
          | states: Map.put(state.states, slot.number, :filtered),
            filtered: merge_filtered([range | state.filtered])
        }

        {:reply, :ok, state}

      _ ->
        {:reply, :ok, state}
    end
  end

  def handle_call(:epoch, _from, state), do: {:reply, state.epoch, state}
  def handle_call(:highest_allocated, _from, state), do: {:reply, state.last, state}
  def handle_call(:declaration, _from, state), do: {:reply, snapshot(state), state}

  def handle_call(:declare, _from, state) do
    sealed = state.sealed or state.complete_through == state.last
    {:reply, snapshot(state), %{state | sealed: sealed}}
  end

  defp roll(%{sealed: false} = state), do: {:ok, state}

  defp roll(state) do
    with {:ok, next} <- successor(state.epoch) do
      {:ok,
       %{
         state
         | epoch: max(next, clock_ms(state.now)),
           previous: %{
             epoch: state.epoch,
             last_emitted: state.last,
             complete_through: state.complete_through
           },
           last: 0,
           complete_through: 0,
           states: %{},
           filtered: [],
           sealed: false
       }}
    end
  end

  # Advances complete_through over EXPORTED slots and discards them.
  defp advance(state) do
    next = state.complete_through + 1

    case Map.get(state.states, next) do
      :exported ->
        advance(%{state | complete_through: next, states: Map.delete(state.states, next)})

      _ ->
        state
    end
  end

  # nil for a slot of another epoch, a number never allocated, or a number at or
  # below complete_through (EXPORTED, already discarded).
  defp state_of(state, %{epoch: epoch, number: number}) do
    if epoch == state.epoch and is_integer(number) and number >= 1 and number <= state.last,
      do: Map.get(state.states, number),
      else: nil
  end

  defp snapshot(state) do
    %{
      epoch: state.epoch,
      last_emitted: state.last,
      complete_through: state.complete_through,
      filtered: state.filtered,
      previous: state.previous
    }
  end

  # Sorted by first slot; adjacent ranges merge only within one Filter epoch.
  # A slot is filtered at most once, so ranges never overlap.
  defp merge_filtered(ranges) do
    ranges
    |> Enum.sort_by(& &1.first)
    |> Enum.reduce([], fn
      range, [%{filter_epoch: same, last: last} = tail | rest]
      when same == range.filter_epoch and last + 1 == range.first ->
        [%{tail | last: range.last} | rest]

      range, acc ->
        [range | acc]
    end)
    |> Enum.reverse()
  end

  defp first_epoch(epoch, previous, _now)
       when is_integer(epoch) and is_integer(previous) and epoch <= previous do
    {:error,
     SequenceError.new(
       "epoch-not-greater",
       "epoch #{epoch} is not greater than the previous epoch #{previous}"
     )}
  end

  defp first_epoch(epoch, _previous, _now) when is_integer(epoch), do: {:ok, epoch}

  defp first_epoch(nil, previous, now) when is_integer(previous) do
    with {:ok, next} <- successor(previous), do: {:ok, max(clock_ms(now), next)}
  end

  defp first_epoch(nil, nil, now), do: {:ok, clock_ms(now)}

  defp successor(value) when value >= @int64_max do
    {:error, SequenceError.new("sequence-overflow", "sequence value #{value} has no successor")}
  end

  defp successor(value), do: {:ok, value + 1}

  defp clock_ms(now) do
    millis = Integer.floor_div(now.(), @nanos_per_ms)

    if millis < 0 or millis > @int64_max,
      do: raise(ArgumentError, "clock is outside the Unix epoch range"),
      else: millis
  end

  defp checked(nil, _name), do: nil

  defp checked(value, _name) when is_integer(value) and value >= 0 and value <= @int64_max,
    do: value

  defp checked(value, name) do
    raise ArgumentError, "#{name} must be an integer in 0..2^63 - 1, got: #{inspect(value)}"
  end
end
