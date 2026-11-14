defmodule Sentinel.Probe.SDK.HeartbeatTest do
  use ExUnit.Case, async: true

  alias Sentinel.Model.V1.ProducerCompletenessSignal
  alias Sentinel.Probe.SDK.Emission.{Sequence, SequenceError}
  alias Sentinel.Probe.SDK.Int128
  alias Sentinel.Probe.SDK.Otlp.Heartbeat

  @start_ns 1_794_000_000_000 * 1_000_000

  setup do
    clock = :atomics.new(1, signed: true)
    :atomics.put(clock, 1, @start_ns)
    now = fn -> :atomics.get(clock, 1) end
    {:ok, sequence} = Sequence.start_link(now: now)
    %{clock: clock, now: now, sequence: sequence}
  end

  # A send function that hands every signal to the test and accepts it.
  defp recording(test \\ self()) do
    fn signal ->
      send(test, {:sent, signal})
      :ok
    end
  end

  # A send function that blocks until the test answers with an outcome.
  defp gated(test \\ self()) do
    fn signal ->
      send(test, {:sent, self(), signal})

      receive do
        {:outcome, outcome} -> outcome
      end
    end
  end

  defp start_heartbeat(context, opts) do
    test = self()

    {:ok, heartbeat} =
      Heartbeat.start_link(
        Keyword.merge(
          [
            sequence: context.sequence,
            now: context.now,
            interval_ms: 0,
            on_error: fn error -> send(test, {:error, error}) end
          ],
          opts
        )
      )

    heartbeat
  end

  defp await_state(heartbeat, predicate) do
    if predicate.(:sys.get_state(heartbeat)), do: :ok, else: await_state(heartbeat, predicate)
  end

  test "the encoded signal sets presence on zero counters and a zero Filter epoch", context do
    heartbeat = start_heartbeat(context, send: recording())
    epoch = Sequence.epoch(context.sequence)

    assert Heartbeat.report(heartbeat) == [
             %{
               sequence_epoch: epoch,
               last_emitted: 0,
               complete_through: 0,
               acknowledged_filter_epoch: 0,
               accepted: true
             }
           ]

    assert_received {:sent, signal}
    decoded = signal |> Protobuf.encode() |> ProducerCompletenessSignal.decode()
    assert decoded.last_emitted == 0
    assert decoded.complete_through == 0
    assert decoded.acknowledged_filter_epoch == 0
    assert decoded.activity == :SOURCE_ACTIVITY_STATE_ACTIVE

    for time <- [decoded.producer_reported_at, decoded.activity_since] do
      assert time.clock_domain_id == "unix"
      assert time.uncertainty_nanoseconds == 0
      assert Int128.to_int(time.nanoseconds) == @start_ns
    end
  end

  test "build_signal sets every field the restatement check compares" do
    signal =
      Heartbeat.build_signal(%{
        sequence_epoch: 7,
        last_emitted: 3,
        complete_through: 1,
        acknowledged_filter_epoch: 2,
        activity: :idle,
        reported_at_ns: 20,
        activity_since_ns: 10
      })

    assert signal.sequence_epoch == 7
    assert signal.last_emitted == 3
    assert signal.complete_through == 1
    assert signal.acknowledged_filter_epoch == 2
    assert signal.activity == :SOURCE_ACTIVITY_STATE_INTENTIONALLY_IDLE
    assert Int128.to_int(signal.producer_reported_at.nanoseconds) == 20
    assert Int128.to_int(signal.activity_since.nanoseconds) == 10
  end

  test "a send that is refused is reported and is not accepted", context do
    heartbeat = start_heartbeat(context, send: fn _ -> {:error, {:http_status, 409, ""}} end)
    assert [%{accepted: false}] = Heartbeat.report(heartbeat)
    assert_received {:error, {:http_status, 409, ""}}
  end

  test "a send that raises is reported and is not accepted", context do
    heartbeat = start_heartbeat(context, send: fn _ -> raise "transport down" end)
    assert [%{accepted: false}] = Heartbeat.report(heartbeat)
    assert_received {:error, %RuntimeError{message: "transport down"}}
    assert Process.alive?(heartbeat)
  end

  test "a Filter epoch regression sends nothing and reports the stable code", context do
    filter = :atomics.new(1, signed: true)
    :atomics.put(filter, 1, 3)

    heartbeat =
      start_heartbeat(context,
        send: recording(),
        filter_epoch: fn -> :atomics.get(filter, 1) end
      )

    {:ok, _} = Sequence.allocate(context.sequence)
    assert [%{acknowledged_filter_epoch: 3}] = Heartbeat.report(heartbeat)
    :atomics.put(filter, 1, 2)
    assert Heartbeat.report(heartbeat) == []
    assert_received {:error, %SequenceError{code: "filter-epoch-regressed"}}
    assert_received {:sent, _}
    refute_received {:sent, _}
  end

  test "reports requested during a send coalesce into one follow-up built after it", context do
    heartbeat = start_heartbeat(context, send: gated())
    first = Task.async(fn -> Heartbeat.report(heartbeat) end)
    assert_receive {:sent, sender, %{last_emitted: 0}}
    followers = for _ <- 1..3, do: Task.async(fn -> Heartbeat.report(heartbeat) end)
    await_state(heartbeat, &(length(&1.waiting) == 3))
    {:ok, _} = Sequence.allocate(context.sequence)
    send(sender, {:outcome, :ok})
    assert_receive {:sent, follow_up, %{last_emitted: 1}}
    send(follow_up, {:outcome, :ok})
    assert [%{last_emitted: 0}] = Task.await(first)
    results = Enum.map(followers, &Task.await/1)
    assert [[%{last_emitted: 1, accepted: true}]] = Enum.uniq(results)
    assert length(results) == 3
    refute_received {:sent, _, _}
  end

  test "declare_idle answers while a send is in flight", context do
    heartbeat = start_heartbeat(context, send: gated())
    running = Task.async(fn -> Heartbeat.report(heartbeat) end)
    assert_receive {:sent, sender, _}
    assert Heartbeat.declare_idle(heartbeat) == :ok
    send(sender, {:outcome, :ok})
    Task.await(running)
    report = Task.async(fn -> Heartbeat.report(heartbeat) end)
    assert_receive {:sent, sender, %{activity: :SOURCE_ACTIVITY_STATE_INTENTIONALLY_IDLE}}
    send(sender, {:outcome, :ok})
    Task.await(report)
  end

  test "stop sends nothing and a late tick is inert", context do
    test = self()

    heartbeat =
      start_heartbeat(context,
        send: recording(),
        interval_ms: 15_000,
        schedule: fn pid, ms ->
          send(test, {:scheduled, pid, ms})
          make_ref()
        end
      )

    assert_received {:scheduled, ^heartbeat, 15_000}
    assert Heartbeat.stop(heartbeat) == :ok
    send(heartbeat, :tick)
    assert Heartbeat.report(heartbeat) == []
    assert Heartbeat.shutdown(heartbeat) == []
    refute_received {:sent, _}
    refute_received {:scheduled, _, _}
  end

  test "stop silences a report that is still sending", context do
    heartbeat = start_heartbeat(context, send: gated())
    running = Task.async(fn -> Heartbeat.report(heartbeat) end)
    assert_receive {:sent, _sender, _}
    assert Heartbeat.stop(heartbeat) == :ok
    assert Task.await(running) == []
    assert Heartbeat.report(heartbeat) == []
  end

  test "shutdown waits for a running report and then sends once", context do
    heartbeat = start_heartbeat(context, send: gated())
    {:ok, slot} = Sequence.allocate(context.sequence)
    running = Task.async(fn -> Heartbeat.report(heartbeat) end)
    assert_receive {:sent, sender, %{complete_through: 0}}
    closing = Task.async(fn -> Heartbeat.shutdown(heartbeat) end)
    await_state(heartbeat, &(&1.closing != nil))
    :ok = Sequence.mark_exported(context.sequence, slot)
    send(sender, {:outcome, :ok})
    assert_receive {:sent, final, %{complete_through: 1, last_emitted: 1}}
    send(final, {:outcome, :ok})
    assert [%{complete_through: 0}] = Task.await(running)
    assert [%{complete_through: 1, accepted: true}] = Task.await(closing)
    assert Heartbeat.report(heartbeat) == []
    refute_received {:sent, _, _}
  end

  test "the schedule runs one report per tick and stops after shutdown", context do
    test = self()

    heartbeat =
      start_heartbeat(context,
        send: recording(),
        interval_ms: 15_000,
        schedule: fn pid, ms ->
          send(test, {:scheduled, pid, ms})
          make_ref()
        end
      )

    assert_received {:scheduled, ^heartbeat, 15_000}
    send(heartbeat, :tick)
    send(heartbeat, :tick)
    Heartbeat.declare_active(heartbeat)
    # The sends run in a task; the second tick coalesces into one follow-up.
    assert_receive {:sent, _}
    assert_receive {:sent, _}
    assert_received {:scheduled, ^heartbeat, 15_000}
    assert_received {:scheduled, ^heartbeat, 15_000}
    assert [%{accepted: true}] = Heartbeat.shutdown(heartbeat)
    assert_received {:sent, _}
    send(heartbeat, :tick)
    Heartbeat.declare_active(heartbeat)
    refute_received {:sent, _}
    refute_received {:scheduled, _, _}
  end

  test "an interval of zero schedules nothing", context do
    test = self()

    start_heartbeat(context,
      send: recording(),
      schedule: fn pid, ms -> send(test, {:scheduled, pid, ms}) end
    )

    refute_received {:scheduled, _, _}
  end

  test "the interval must be zero or between one second and the silence bound", context do
    for interval <- [-1, 999, 60_000, 1.5] do
      assert_raise ArgumentError, fn ->
        Heartbeat.start_link(sequence: context.sequence, send: recording(), interval_ms: interval)
      end
    end

    for interval <- [0, 1_000, 59_999] do
      assert {:ok, _} =
               Heartbeat.start_link(
                 sequence: context.sequence,
                 send: recording(),
                 interval_ms: interval,
                 schedule: fn _, _ -> make_ref() end
               )
    end

    assert Heartbeat.default_interval_ms() == 15_000
  end

  test "a send function or an OTLP endpoint is required", context do
    assert_raise ArgumentError, fn -> Heartbeat.start_link(sequence: context.sequence) end
  end
end
