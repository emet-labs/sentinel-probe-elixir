defmodule Sentinel.Probe.SDK.SequenceTest do
  use ExUnit.Case, async: true

  alias Sentinel.Probe.SDK.Emission.{Sequence, SequenceError}

  @start_ms 1_794_000_000_000
  @int64_max 9_223_372_036_854_775_807

  setup do
    clock = :atomics.new(1, signed: true)
    :atomics.put(clock, 1, @start_ms * 1_000_000)
    %{clock: clock}
  end

  defp start(clock, opts) do
    Sequence.start_link([now: fn -> :atomics.get(clock, 1) end] ++ opts)
  end

  defp start!(clock, opts \\ []) do
    {:ok, sequence} = start(clock, opts)
    sequence
  end

  defp set_ms(clock, ms), do: :atomics.put(clock, 1, ms * 1_000_000)

  defp allocate!(sequence) do
    {:ok, slot} = Sequence.allocate(sequence)
    slot
  end

  defp counters(declaration),
    do: Map.take(declaration, [:epoch, :last_emitted, :complete_through])

  test "the epoch is the clock in Unix milliseconds", %{clock: clock} do
    :atomics.put(clock, 1, @start_ms * 1_000_000 + 999_999)
    sequence = start!(clock)
    assert Sequence.epoch(sequence) == @start_ms
    assert Sequence.allocate(sequence) == {:ok, %{epoch: @start_ms, number: 1}}
  end

  test "an explicit epoch overrides the clock", %{clock: clock} do
    assert Sequence.allocate(start!(clock, epoch: 42)) == {:ok, %{epoch: 42, number: 1}}
  end

  test "a previous epoch the clock is ahead of keeps the clock", %{clock: clock} do
    assert Sequence.epoch(start!(clock, previous_epoch: @start_ms - 60_000)) == @start_ms
  end

  test "a previous epoch the clock is behind takes previous plus one", %{clock: clock} do
    assert Sequence.epoch(start!(clock, previous_epoch: @start_ms)) == @start_ms + 1
    assert Sequence.epoch(start!(clock, previous_epoch: @start_ms + 60_000)) == @start_ms + 60_001
  end

  test "an override that is not greater than the previous epoch is refused", %{clock: clock} do
    assert {:error, %SequenceError{code: "epoch-not-greater"}} =
             start(clock, epoch: 100, previous_epoch: 100)

    assert {:error, %SequenceError{code: "epoch-not-greater"}} =
             start(clock, epoch: 50, previous_epoch: 100)

    assert Sequence.epoch(start!(clock, epoch: 101, previous_epoch: 100)) == 101
  end

  test "negative or oversized epochs are refused at start", %{clock: clock} do
    assert_raise ArgumentError, fn -> start(clock, epoch: -1) end
    assert_raise ArgumentError, fn -> start(clock, previous_epoch: -1) end
    assert_raise ArgumentError, fn -> start(clock, epoch: @int64_max + 1) end
  end

  test "a fresh epoch declares zero with no filtered range and no previous epoch", %{clock: clock} do
    sequence = start!(clock)

    assert Sequence.declaration(sequence) == %{
             epoch: @start_ms,
             last_emitted: 0,
             complete_through: 0,
             filtered: [],
             previous: nil
           }

    assert Sequence.highest_allocated(sequence) == 0
  end

  test "allocation numbers slots from one within the epoch", %{clock: clock} do
    sequence = start!(clock)
    assert Enum.map(1..3, fn _ -> allocate!(sequence).number end) == [1, 2, 3]
    assert Sequence.highest_allocated(sequence) == 3
  end

  test "complete_through advances only over a whole exported prefix", %{clock: clock} do
    sequence = start!(clock)
    [one, two, three] = Enum.map(1..3, fn _ -> allocate!(sequence) end)
    :ok = Sequence.mark_exported(sequence, three)
    assert Sequence.declaration(sequence).complete_through == 0
    :ok = Sequence.mark_exported(sequence, one)
    assert Sequence.declaration(sequence).complete_through == 1
    :ok = Sequence.mark_exported(sequence, two)
    assert Sequence.declaration(sequence).complete_through == 3
  end

  test "a failed export stalls the prefix until the slot is exported", %{clock: clock} do
    sequence = start!(clock)
    [one, two, three] = Enum.map(1..3, fn _ -> allocate!(sequence) end)
    Sequence.mark_exported(sequence, one)
    Sequence.mark_failed(sequence, two)
    Sequence.mark_exported(sequence, three)
    assert Sequence.declaration(sequence).complete_through == 1
    Sequence.mark_exported(sequence, two)
    assert Sequence.declaration(sequence).complete_through == 3
  end

  test "a filtered slot stalls the prefix and records its Filter epoch", %{clock: clock} do
    sequence = start!(clock)
    [one, two, three] = Enum.map(1..3, fn _ -> allocate!(sequence) end)
    Sequence.mark_exported(sequence, one)
    :ok = Sequence.mark_filtered(sequence, two, 4)
    Sequence.mark_exported(sequence, three)
    declaration = Sequence.declaration(sequence)
    assert declaration.complete_through == 1
    assert declaration.filtered == [%{first: 2, last: 2, filter_epoch: 4}]
    Sequence.mark_exported(sequence, two)
    assert Sequence.declaration(sequence).complete_through == 1, "FILTERED is terminal until #542"
  end

  test "adjacent filtered slots merge only within one Filter epoch", %{clock: clock} do
    sequence = start!(clock)
    slots = Enum.map(1..7, fn _ -> allocate!(sequence) end)
    Sequence.mark_filtered(sequence, Enum.at(slots, 3), 4)
    Sequence.mark_filtered(sequence, Enum.at(slots, 1), 4)
    Sequence.mark_filtered(sequence, Enum.at(slots, 2), 4)
    Sequence.mark_filtered(sequence, Enum.at(slots, 4), 5)
    Sequence.mark_filtered(sequence, Enum.at(slots, 6), 5)

    assert Sequence.declaration(sequence).filtered == [
             %{first: 2, last: 4, filter_epoch: 4},
             %{first: 5, last: 5, filter_epoch: 5},
             %{first: 7, last: 7, filter_epoch: 5}
           ]
  end

  test "marks for unknown, foreign or settled slots are no-ops", %{clock: clock} do
    sequence = start!(clock)
    first = allocate!(sequence)
    second = allocate!(sequence)
    Sequence.mark_exported(sequence, %{first | epoch: first.epoch + 1})
    Sequence.mark_exported(sequence, %{first | number: 3})
    Sequence.mark_exported(sequence, %{first | number: 0})
    assert Sequence.declaration(sequence).complete_through == 0
    Sequence.mark_exported(sequence, first)
    Sequence.mark_failed(sequence, first)
    Sequence.mark_filtered(sequence, first, 1)
    assert Sequence.declaration(sequence).complete_through == 1
    assert Sequence.declaration(sequence).filtered == []
    Sequence.mark_filtered(sequence, second, 2)
    Sequence.mark_failed(sequence, second)
    Sequence.mark_exported(sequence, second)
    assert Sequence.declaration(sequence).complete_through == 1
    assert Sequence.declaration(sequence).filtered == [%{first: 2, last: 2, filter_epoch: 2}]
  end

  test "a failed slot cannot be marked filtered", %{clock: clock} do
    sequence = start!(clock)
    slot = allocate!(sequence)
    Sequence.mark_failed(sequence, slot)
    Sequence.mark_filtered(sequence, slot, 1)
    assert Sequence.declaration(sequence).filtered == []
  end

  test "declaration never seals and the next allocation stays in the epoch", %{clock: clock} do
    sequence = start!(clock)
    Sequence.mark_exported(sequence, allocate!(sequence))
    Sequence.declaration(sequence)
    assert allocate!(sequence) == %{epoch: @start_ms, number: 2}
  end

  test "declare with a slot in flight does not seal", %{clock: clock} do
    sequence = start!(clock)
    allocate!(sequence)

    assert counters(Sequence.declare(sequence)) == %{
             epoch: @start_ms,
             last_emitted: 1,
             complete_through: 0
           }

    assert allocate!(sequence) == %{epoch: @start_ms, number: 2}
  end

  test "a declared close seals the epoch and the next allocation rolls", %{clock: clock} do
    sequence = start!(clock)
    Sequence.mark_exported(sequence, allocate!(sequence))

    assert counters(Sequence.declare(sequence)) == %{
             epoch: @start_ms,
             last_emitted: 1,
             complete_through: 1
           }

    assert allocate!(sequence) == %{epoch: @start_ms + 1, number: 1}
    rolled = Sequence.declaration(sequence)
    assert rolled.previous == %{epoch: @start_ms, last_emitted: 1, complete_through: 1}
    assert counters(rolled) == %{epoch: @start_ms + 1, last_emitted: 1, complete_through: 0}
  end

  test "the roll takes the clock when it is ahead of the next epoch", %{clock: clock} do
    sequence = start!(clock)
    Sequence.mark_exported(sequence, allocate!(sequence))
    Sequence.declare(sequence)
    set_ms(clock, @start_ms + 60_000)
    assert allocate!(sequence) == %{epoch: @start_ms + 60_000, number: 1}
  end

  test "an empty epoch declared closed rolls on the next allocation", %{clock: clock} do
    sequence = start!(clock)

    assert counters(Sequence.declare(sequence)) == %{
             epoch: @start_ms,
             last_emitted: 0,
             complete_through: 0
           }

    assert allocate!(sequence) == %{epoch: @start_ms + 1, number: 1}

    assert Sequence.declaration(sequence).previous == %{
             epoch: @start_ms,
             last_emitted: 0,
             complete_through: 0
           }
  end

  test "after the roll a mark for the closed epoch is a no-op", %{clock: clock} do
    sequence = start!(clock)
    slot = allocate!(sequence)
    Sequence.mark_failed(sequence, slot)
    Sequence.mark_exported(sequence, slot)
    Sequence.declare(sequence)
    next = allocate!(sequence)
    Sequence.mark_filtered(sequence, next, 2)
    Sequence.mark_exported(sequence, slot)
    declaration = Sequence.declaration(sequence)
    assert declaration.filtered == [%{first: 1, last: 1, filter_epoch: 2}]
    assert counters(declaration) == %{epoch: @start_ms + 1, last_emitted: 1, complete_through: 0}
  end

  test "an epoch past the maximum is an overflow, never a wrap", %{clock: clock} do
    sequence = start!(clock, epoch: @int64_max)
    Sequence.declare(sequence)
    assert {:error, %SequenceError{code: "sequence-overflow"}} = Sequence.allocate(sequence)
    assert Sequence.epoch(sequence) == @int64_max

    assert {:error, %SequenceError{code: "sequence-overflow"}} =
             start(clock, previous_epoch: @int64_max)
  end

  test "no slot is ever allocated in an epoch whose close was declared", %{clock: clock} do
    sequence = start!(clock)

    declarer =
      Task.async(fn ->
        Stream.repeatedly(fn -> Sequence.declare(sequence) end)
        |> Enum.reduce_while(%{}, fn declaration, closed ->
          closed =
            if declaration.complete_through == declaration.last_emitted,
              do: Map.put(closed, declaration.epoch, declaration.last_emitted),
              else: closed

          receive do
            :done -> {:halt, closed}
          after
            0 -> {:cont, closed}
          end
        end)
      end)

    workers =
      for _ <- 1..4 do
        Task.async(fn ->
          for _ <- 1..500 do
            slot = allocate!(sequence)
            :ok = Sequence.mark_exported(sequence, slot)
            slot
          end
        end)
      end

    slots = workers |> Enum.flat_map(&Task.await(&1, :infinity))
    send(declarer.pid, :done)
    closed = Task.await(declarer, :infinity)
    assert length(Enum.uniq(slots)) == length(slots)

    for slot <- slots, Map.has_key?(closed, slot.epoch) do
      close = Map.fetch!(closed, slot.epoch)
      assert slot.number <= close, "#{inspect(slot)} allocated after its epoch closed at #{close}"
    end
  end
end
