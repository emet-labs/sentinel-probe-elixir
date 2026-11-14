defmodule Sentinel.Probe.SDK.FilterStoreTest do
  use ExUnit.Case, async: true
  alias Sentinel.Model.V1.{EventFilter, SpecificationFilter}
  alias Sentinel.Probe.SDK.Client.FilterStore

  defp key(n), do: {:sentinel_test_filter, n, self()}

  defp new_store(n, initial \\ nil), do: FilterStore.new(key(n), initial)

  defp filter(epoch), do: %EventFilter{epoch: epoch, specifications: []}

  test "get returns nil before the first set" do
    store = new_store(:get_nil)
    assert FilterStore.get(store) == nil
  after
    FilterStore.delete(struct(FilterStore, key: key(:get_nil)))
  end

  test "set returns true on the first set" do
    store = new_store(:first)
    assert FilterStore.set(store, filter(5)) == true
    assert FilterStore.get(store) != nil
  after
    FilterStore.delete(struct(FilterStore, key: key(:first)))
  end

  test "set returns false when the epoch is unchanged" do
    store = new_store(:unchanged)
    assert FilterStore.set(store, filter(5)) == true
    assert FilterStore.set(store, filter(5)) == false
  after
    FilterStore.delete(struct(FilterStore, key: key(:unchanged)))
  end

  test "set returns true when the epoch changes" do
    store = new_store(:changed)
    assert FilterStore.set(store, filter(5)) == true
    assert FilterStore.set(store, filter(6)) == true
  after
    FilterStore.delete(struct(FilterStore, key: key(:changed)))
  end

  test "epoch compares by value, not by identity" do
    store = new_store(:by_value)
    f1 = %EventFilter{epoch: 7, specifications: [%SpecificationFilter{specification_id: "a"}]}
    f2 = %EventFilter{epoch: 7, specifications: [%SpecificationFilter{specification_id: "b"}]}
    assert FilterStore.set(store, f1) == true
    # Same epoch, different struct — must NOT report updated.
    assert FilterStore.set(store, f2) == false
    assert FilterStore.get(store).specifications == [%SpecificationFilter{specification_id: "a"}]
  after
    FilterStore.delete(struct(FilterStore, key: key(:by_value)))
  end

  test "epoch 0 is a legitimate epoch, not absent" do
    store = new_store(:zero)
    assert FilterStore.set(store, filter(0)) == true
    assert FilterStore.epoch(store) == 0
    refute FilterStore.epoch(store) == nil
  after
    FilterStore.delete(struct(FilterStore, key: key(:zero)))
  end

  test "epoch returns nil for no filter" do
    store = new_store(:nil_epoch)
    assert FilterStore.epoch(store) == nil
  after
    FilterStore.delete(struct(FilterStore, key: key(:nil_epoch)))
  end

  test "should_refresh: no filter held means refresh" do
    store = new_store(:refresh_no_filter)
    assert FilterStore.should_refresh(store, 1) == true
  after
    FilterStore.delete(struct(FilterStore, key: key(:refresh_no_filter)))
  end

  test "should_refresh: no filter held with nil epoch means refresh" do
    store = new_store(:refresh_nil)
    assert FilterStore.should_refresh(store, nil) == true
  after
    FilterStore.delete(struct(FilterStore, key: key(:refresh_nil)))
  end

  test "should_refresh: different epoch means refresh" do
    store = new_store(:refresh_diff)
    FilterStore.set(store, filter(5))
    assert FilterStore.should_refresh(store, 6) == true
  after
    FilterStore.delete(struct(FilterStore, key: key(:refresh_diff)))
  end

  test "should_refresh: same epoch means no refresh" do
    store = new_store(:refresh_same)
    FilterStore.set(store, filter(5))
    assert FilterStore.should_refresh(store, 5) == false
  after
    FilterStore.delete(struct(FilterStore, key: key(:refresh_same)))
  end

  test "both no-epoch filters still counts as update on first set" do
    store = new_store(:both_nil)
    f1 = %EventFilter{epoch: nil}
    f2 = %EventFilter{epoch: nil}
    assert FilterStore.set(store, f1) == true
    assert FilterStore.set(store, f2) == false
  after
    FilterStore.delete(struct(FilterStore, key: key(:both_nil)))
  end

  defp labelled(label, epoch) do
    %EventFilter{epoch: epoch, specifications: [%SpecificationFilter{specification_id: label}]}
  end

  test "first set applies whatever the epoch" do
    for {epoch, n} <- [{7, :first_7}, {0, :first_0}, {nil, :first_nil}] do
      store = new_store(n)
      first = labelled("first", epoch)
      assert FilterStore.set(store, first) == true
      assert FilterStore.get(store) == first
      FilterStore.delete(store)
    end
  end

  test "a lower epoch is refused and the held filter kept" do
    store = new_store(:lower)
    held = labelled("a", 5)
    FilterStore.set(store, held)
    assert FilterStore.set(store, labelled("b", 4)) == false
    assert FilterStore.set(store, labelled("zero", 0)) == false
    assert FilterStore.get(store) == held
    assert FilterStore.epoch(store) == 5
  after
    FilterStore.delete(struct(FilterStore, key: key(:lower)))
  end

  test "an epoch-less filter after an epoch-bearing one is refused" do
    for {epoch, n} <- [{5, :epochless_5}, {0, :epochless_0}] do
      held = labelled("a", epoch)
      store = new_store(n, held)
      assert FilterStore.set(store, labelled("none", nil)) == false
      assert FilterStore.get(store) == held
      FilterStore.delete(store)
    end
  end

  test "a higher epoch after epoch 0 replaces" do
    store = new_store(:after_zero, labelled("zero", 0))
    one = labelled("one", 1)
    assert FilterStore.set(store, one) == true
    assert FilterStore.get(store) == one
  after
    FilterStore.delete(struct(FilterStore, key: key(:after_zero)))
  end

  test "epochs order as unsigned 64-bit values" do
    store = new_store(:unsigned)

    for epoch <- [Bitwise.bsl(1, 63) - 1, Bitwise.bsl(1, 63), Bitwise.bsl(1, 64) - 1] do
      assert FilterStore.set(store, labelled("e#{epoch}", epoch)) == true
    end

    top = FilterStore.get(store)
    assert FilterStore.set(store, labelled("back", Bitwise.bsl(1, 63))) == false
    assert FilterStore.get(store) == top
    assert FilterStore.should_refresh(store, Bitwise.bsl(1, 63)) == false
  after
    FilterStore.delete(struct(FilterStore, key: key(:unsigned)))
  end

  test "a seeded store refuses a regression" do
    seed = labelled("seed", 9)
    store = new_store(:seeded, seed)
    assert FilterStore.set(store, labelled("old", 8)) == false
    assert FilterStore.get(store) == seed
  after
    FilterStore.delete(struct(FilterStore, key: key(:seeded)))
  end

  test "a refused set does not move later comparisons" do
    store = new_store(:refusal, labelled("a", 5))
    assert FilterStore.set(store, labelled("b", 2)) == false
    assert FilterStore.set(store, labelled("c", 3)) == false
    assert FilterStore.should_refresh(store, 4) == false
    six = labelled("d", 6)
    assert FilterStore.set(store, six) == true
    assert FilterStore.get(store) == six
  after
    FilterStore.delete(struct(FilterStore, key: key(:refusal)))
  end

  test "should_refresh is true only for a strictly newer epoch" do
    store = new_store(:refresh_newer, labelled("a", 5))
    assert FilterStore.should_refresh(store, 4) == false
    assert FilterStore.should_refresh(store, 5) == false
    assert FilterStore.should_refresh(store, 6) == true
  after
    FilterStore.delete(struct(FilterStore, key: key(:refresh_newer)))
  end

  # Worst-case latency under contention, by deterministic interleaving: the
  # writer is held suspended until every contender has queued its set. Each set
  # must cost exactly one writer message (no lock, no retry, no back-off sleep),
  # so a set waits only for the sets queued ahead of it.
  test "a contended set waits only for the sets queued ahead of it" do
    store = new_store(:contended)
    writer = FilterStore.writer(store)
    contenders = 16
    :ok = :sys.statistics(writer, true)
    :ok = :sys.suspend(writer)

    tasks =
      for epoch <- 1..contenders do
        Task.async(fn -> FilterStore.set(store, filter(epoch)) end)
      end

    await_queue(writer, contenders)
    :ok = :sys.resume(writer)
    results = Task.await_many(tasks, :infinity)

    {:ok, stats} = :sys.statistics(writer, :get)
    assert Keyword.fetch!(stats, :messages_in) == contenders
    assert Enum.any?(results)
    assert FilterStore.epoch(store) == contenders
  after
    FilterStore.delete(struct(FilterStore, key: key(:contended)))
  end

  test "a killed writer is started again and the next set applies" do
    store = new_store(:killed)
    assert FilterStore.set(store, filter(5))
    writer = FilterStore.writer(store)
    ref = Process.monitor(writer)
    Process.exit(writer, :kill)
    assert_receive {:DOWN, ^ref, :process, ^writer, :killed}

    # Neither path exits with :noproc or drops the Filter: each starts a writer.
    assert FilterStore.set(store, filter(6)) == true
    assert FilterStore.epoch(store) == 6
    restarted = FilterStore.writer(store)
    refute restarted == writer
    assert Process.alive?(restarted)

    ref = Process.monitor(restarted)
    Process.exit(restarted, :kill)
    assert_receive {:DOWN, ^ref, :process, ^restarted, :killed}
    assert FilterStore.set_async(store, filter(7)) == :ok
    :sys.get_state(FilterStore.writer(store))
    assert FilterStore.epoch(store) == 7
  after
    FilterStore.delete(struct(FilterStore, key: key(:killed)))
  end

  test "a deleted store refuses sets and starts no writer until new/2" do
    k = key(:deleted)
    store = FilterStore.new(k, filter(3))
    :ok = FilterStore.delete(store)
    assert FilterStore.deleted?(store)
    assert FilterStore.get(store) == nil
    assert FilterStore.running_writer(store) == nil
    assert FilterStore.set(store, filter(4)) == false
    assert FilterStore.set_async(store, filter(5)) == :ok
    assert FilterStore.running_writer(store) == nil
    assert FilterStore.get(store) == nil

    store = FilterStore.new(k)
    refute FilterStore.deleted?(store)
    assert FilterStore.set(store, filter(6)) == true
    assert FilterStore.epoch(store) == 6
  after
    FilterStore.delete(struct(FilterStore, key: key(:deleted)))
  end

  test "concurrent new/2 for one key starts exactly one writer" do
    k = key(:racing_new)
    gate = make_ref()
    test_pid = self()

    tasks =
      for _ <- 1..16 do
        Task.async(fn ->
          send(test_pid, {:ready, self()})

          receive do
            ^gate -> FilterStore.writer(FilterStore.new(k))
          end
        end)
      end

    # Release every task at once, so the starts race.
    for task <- tasks do
      assert_receive {:ready, pid} when pid == task.pid
    end

    Enum.each(tasks, &send(&1.pid, gate))
    writers = Task.await_many(tasks, :infinity)
    assert [one] = Enum.uniq(writers)
    assert FilterStore.running_writer(%FilterStore{key: k}) == one
  after
    FilterStore.delete(struct(FilterStore, key: key(:racing_new)))
  end

  # Spins on the mailbox length; it never sleeps.
  defp await_queue(pid, n) do
    case Process.info(pid, :message_queue_len) do
      {:message_queue_len, ^n} -> :ok
      _ -> await_queue(pid, n)
    end
  end

  test "concurrent sets from many processes end at the highest epoch" do
    store = new_store(:concurrent)
    writers = 8
    per_writer = 50
    test_pid = self()

    observer =
      spawn_link(fn ->
        observe = fn observe, last ->
          receive do
            :stop -> send(test_pid, {:observed, :ok})
          after
            0 ->
              epoch = FilterStore.epoch(store)

              cond do
                epoch == nil -> observe.(observe, last)
                epoch < last -> send(test_pid, {:observed, {:regressed, last, epoch}})
                true -> observe.(observe, epoch)
              end
          end
        end

        observe.(observe, -1)
      end)

    1..writers
    |> Enum.map(fn offset ->
      Task.async(fn ->
        for i <- per_writer..1//-1 do
          FilterStore.set(store, filter((i - 1) * writers + offset))
        end
      end)
    end)
    |> Task.await_many()

    send(observer, :stop)
    assert_receive {:observed, :ok}
    assert FilterStore.epoch(store) == writers * per_writer
  after
    FilterStore.delete(struct(FilterStore, key: key(:concurrent)))
  end
end
