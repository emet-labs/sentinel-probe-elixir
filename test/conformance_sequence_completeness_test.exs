defmodule Sentinel.Probe.SDK.ConformanceSequenceCompletenessTest do
  # Executes the shared sequence_completeness suite (#461) through the real
  # Sequence allocator, completeness Heartbeat and PartialSuccess decoder. The
  # suite is found through the manifest.
  use ExUnit.Case, async: true

  alias Sentinel.Model.V1.ProducerCompletenessSignal
  alias Sentinel.Probe.SDK.Emission.{Sequence, SequenceError}
  alias Sentinel.Probe.SDK.Int128
  alias Sentinel.Probe.SDK.Otlp.{Heartbeat, PartialSuccess}

  @root Path.expand("../../../testdata/probe-sdk-conformance", __DIR__)

  defp read(name), do: @root |> Path.join(name) |> File.read!() |> Jason.decode!()

  defp cases do
    manifest = read("manifest-v1.json")
    assert manifest["format_version"] == "1.4.0"
    [entry] = Enum.filter(manifest["suites"], &(&1["kind"] == "sequence_completeness"))
    suite = read(entry["path"])
    assert suite["format_version"] == "1.3.0"
    assert suite["kind"] == "sequence_completeness"
    cases = suite["cases"]
    assert length(Enum.uniq_by(cases, & &1["id"])) == length(cases)

    for vector <- cases do
      assert Map.has_key?(vector, "steps") != Map.has_key?(vector, "response_hex"), vector["id"]
    end

    cases
  end

  test "sequence_completeness scenarios drive the allocator and the heartbeat" do
    scenarios = Enum.filter(cases(), &Map.has_key?(&1, "steps"))
    assert scenarios != []

    for vector <- scenarios do
      world = start_world(vector["id"], vector["start"])

      vector["steps"]
      |> Enum.with_index()
      |> Enum.reduce(world, fn {step, index}, world ->
        step(world, step, "#{vector["id"]} step #{index} #{step["op"]}")
      end)
    end
  end

  test "sequence_completeness partial-success cases decode rejected_spans" do
    responses = Enum.filter(cases(), &Map.has_key?(&1, "response_hex"))
    assert responses != []

    for vector <- responses do
      body = Base.decode16!(vector["response_hex"], case: :lower)

      if vector["undecodable"] == true do
        assert {:error, _} = PartialSuccess.rejected_spans(body), vector["id"]
      else
        assert PartialSuccess.rejected_spans(body) == {:ok, uint(vector["rejected_spans"])},
               vector["id"]
      end
    end
  end

  # The world is one allocator and one heartbeat over an injected clock,
  # Filter-epoch reader and send function, plus the last signal sent.
  defp start_world(id, start) do
    clock = :atomics.new(1, signed: true)
    {:ok, script} = Agent.start_link(fn -> %{outcomes: [], sent: [], errors: [], filter: nil} end)
    world = %{id: id, clock: clock, script: script, previous: nil}
    set_clock(world, start["clock_unix_ms"])
    {:ok, sequence} = start_sequence(world, uint(start["epoch"]), uint(start["previous_epoch"]))
    Map.merge(world, %{sequence: sequence, heartbeat: start_heartbeat(world, sequence)})
  end

  defp step(world, %{"op" => "allocate"} = step, where) do
    case step do
      %{"expect_error" => code} ->
        assert {:error, %SequenceError{code: ^code}} = Sequence.allocate(world.sequence), where

      %{"expect" => %{"epoch" => epoch, "number" => number}} ->
        assert Sequence.allocate(world.sequence) ==
                 {:ok, %{epoch: uint(epoch), number: uint(number)}},
               where
    end

    world
  end

  defp step(world, %{"op" => "export_ok"} = step, _where) do
    :ok = Sequence.mark_exported(world.sequence, slot(step))
    world
  end

  defp step(world, %{"op" => "export_fail"} = step, _where) do
    :ok = Sequence.mark_failed(world.sequence, slot(step))
    world
  end

  defp step(world, %{"op" => "filter_drop"} = step, _where) do
    :ok = Sequence.mark_filtered(world.sequence, slot(step), uint(step["filter_epoch"]))
    world
  end

  defp step(world, %{"op" => "set_filter_epoch", "epoch" => epoch}, _where) do
    Agent.update(world.script, &%{&1 | filter: uint(epoch)})
    world
  end

  defp step(world, %{"op" => "clock", "unix_ms" => ms}, _where) do
    set_clock(world, ms)
    world
  end

  defp step(world, %{"op" => "idle"}, _where) do
    :ok = Heartbeat.declare_idle(world.heartbeat)
    world
  end

  defp step(world, %{"op" => "active"}, _where) do
    :ok = Heartbeat.declare_active(world.heartbeat)
    world
  end

  defp step(world, %{"op" => "tick"} = step, where),
    do: report(world, step, where, &Heartbeat.report/1)

  defp step(world, %{"op" => "shutdown"} = step, where),
    do: report(world, step, where, &Heartbeat.shutdown/1)

  defp step(world, %{"op" => "restart"} = step, where) do
    set_clock(world, step["clock_unix_ms"])

    previous =
      case step["previous_epoch"] do
        "inherit" -> Sequence.epoch(world.sequence)
        value -> uint(value)
      end

    case {start_sequence(world, uint(step["epoch"]), previous), step["expect_error"]} do
      {{:error, %SequenceError{code: code}}, code} ->
        world

      {{:ok, sequence}, nil} ->
        :ok = Heartbeat.stop(world.heartbeat)
        %{world | sequence: sequence, heartbeat: start_heartbeat(world, sequence)}

      {result, expected} ->
        flunk("#{where}: restart gave #{inspect(result)}, expected error #{inspect(expected)}")
    end
  end

  defp report(world, step, where, call) do
    sends = step["send"]
    Agent.update(world.script, &%{&1 | outcomes: sends, sent: [], errors: []})
    reports = call.(world.heartbeat)
    %{sent: sent, errors: errors, outcomes: left} = Agent.get(world.script, & &1)
    sent = Enum.reverse(sent)
    expected = step["expect"]
    assert length(sent) == length(expected), "#{where}: signals sent"
    assert left == [], "#{where}: unused send outcomes"
    assert Enum.map(reports, & &1.accepted) == Enum.map(sends, &(&1 == "accepted")), where

    world =
      sent
      |> Enum.zip(expected)
      |> Enum.with_index()
      |> Enum.reduce(world, fn {{signal, want}, position}, world ->
        assert_signal(world, signal, want, "#{where} signal #{position}")
      end)

    codes = for %SequenceError{code: code} <- errors, do: code
    assert codes == List.wrap(step["expect_error"]), "#{where}: error codes"
    refusals = Enum.count(errors, &(&1 == :not_accepted))
    assert refusals == Enum.count(sends, &(&1 == "not_accepted")), "#{where}: refusals"

    filtered =
      Enum.map(step["filtered"], fn range ->
        %{
          first: uint(range["first"]),
          last: uint(range["last"]),
          filter_epoch: uint(range["filter_epoch"])
        }
      end)

    assert Sequence.declaration(world.sequence).filtered == filtered, "#{where}: filtered"
    world
  end

  defp assert_signal(world, signal, want, where) do
    # Decode the wire bytes, so presence is checked as Sentinel sees it.
    wire = Protobuf.encode(signal)
    decoded = ProducerCompletenessSignal.decode(wire)
    assert decoded.sequence_epoch == uint(want["sequence_epoch"]), "#{where} sequence_epoch"
    assert decoded.last_emitted == uint(want["last_emitted"]), "#{where} last_emitted"
    assert decoded.complete_through == uint(want["complete_through"]), "#{where} complete_through"

    assert decoded.acknowledged_filter_epoch == uint(want["acknowledged_filter_epoch"]),
           "#{where} acknowledged_filter_epoch"

    assert decoded.activity == activity(want["activity"]), "#{where} activity"

    for {time, key} <- [
          {decoded.producer_reported_at, "reported_at_ns"},
          {decoded.activity_since, "activity_since_ns"}
        ] do
      assert time.clock_domain_id == "unix", "#{where} #{key} clock domain"
      assert time.uncertainty_nanoseconds == 0, "#{where} #{key} uncertainty"
      assert Int128.to_int(time.nanoseconds) == uint(want[key]), "#{where} #{key}"
    end

    assert restates?(world.previous, signal) == want["restates_previous"],
           "#{where} restates_previous"

    %{world | previous: signal}
  end

  # The signal, re-encoded with the previous signal's report nanoseconds, is
  # byte-identical to the previous signal's encoding.
  defp restates?(nil, _signal), do: false

  defp restates?(previous, signal) do
    aligned = %{
      signal
      | producer_reported_at: %{
          signal.producer_reported_at
          | nanoseconds: previous.producer_reported_at.nanoseconds
        }
    }

    Protobuf.encode(aligned) == Protobuf.encode(previous)
  end

  defp start_sequence(world, epoch, previous_epoch) do
    Sequence.start_link(
      now: fn -> :atomics.get(world.clock, 1) end,
      epoch: epoch,
      previous_epoch: previous_epoch
    )
  end

  defp start_heartbeat(world, sequence) do
    script = world.script

    {:ok, heartbeat} =
      Heartbeat.start_link(
        sequence: sequence,
        send: fn signal ->
          Agent.get_and_update(script, fn state ->
            {outcome, rest} =
              case state.outcomes do
                [next | rest] -> {next, rest}
                [] -> {"unscripted", []}
              end

            {outcome, %{state | outcomes: rest, sent: [signal | state.sent]}}
          end)
          |> case do
            "accepted" -> :ok
            _ -> {:error, :not_accepted}
          end
        end,
        filter_epoch: fn -> Agent.get(script, & &1.filter) end,
        now: fn -> :atomics.get(world.clock, 1) end,
        interval_ms: 0,
        on_error: fn error -> Agent.update(script, &%{&1 | errors: &1.errors ++ [error]}) end
      )

    heartbeat
  end

  defp set_clock(world, ms), do: :atomics.put(world.clock, 1, uint(ms) * 1_000_000)

  defp slot(step), do: %{epoch: uint(step["epoch"]), number: uint(step["number"])}

  defp uint(nil), do: nil
  defp uint(value) when is_binary(value), do: String.to_integer(value)

  defp activity("active"), do: :SOURCE_ACTIVITY_STATE_ACTIVE
  defp activity("idle"), do: :SOURCE_ACTIVITY_STATE_INTENTIONALLY_IDLE
end
