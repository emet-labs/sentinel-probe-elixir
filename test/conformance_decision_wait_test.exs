defmodule Sentinel.Probe.SDK.ConformanceDecisionWaitTest do
  # The decision_wait suite (manifest-v1.json, 1.4.0) through the production log receiver and
  # Client.log_decider. Only the receiver's `decision_timer:` is injected; it runs in the asking
  # process. `deliver` POSTs the answer golden to the receiver origin from inside the timer, and
  # `expire` sends `{:decision_timeout, ref}`. The timer is armed in whole milliseconds. No test
  # reads a wall clock.
  use ExUnit.Case, async: true

  alias Sentinel.Model.V1.{EventFilter, EventMatch, ProducerEvent, SpecificationFilter}
  alias Sentinel.Probe.SDK.Client.Client
  alias Sentinel.Probe.SDK.Enforcement.Gate
  alias Sentinel.Probe.SDK.Otlp.{Http, Receiver}
  alias Sentinel.Probe.V1.DecideRequest

  @root Path.expand("../testdata/probe-sdk-conformance", __DIR__)

  defp load(name), do: @root |> Path.join(name) |> File.read!() |> Jason.decode!()

  defp cases do
    manifest = load("manifest-v1.json")
    assert manifest["format_version"] == "1.4.0"
    assert [entry] = Enum.filter(manifest["suites"], &(&1["kind"] == "decision_wait"))
    suite = load(entry["path"])
    assert suite["format_version"] == "1.2.0"
    assert suite["kind"] == "decision_wait"
    assert suite["cases"] != []
    suite["cases"]
  end

  defp start(vector) do
    answer = vector["answer"] |> Path.expand(@root) |> File.read!()
    test_pid = self()
    on_wait = vector["on_wait"]

    timer = fn timeout_ms, ref ->
      send(test_pid, {:armed, timeout_ms})
      receiver = Process.get(:decision_wait_receiver)

      for step <- on_wait do
        case step do
          "deliver" -> deliver(receiver, answer)
          "expire" -> send(self(), {:decision_timeout, ref})
        end
      end

      fn -> :ok end
    end

    {:ok, receiver} = Receiver.start_link(decision_timer: timer)
    Process.unlink(receiver)
    Process.put(:decision_wait_receiver, receiver)
    on_exit(fn -> if Process.alive?(receiver), do: Receiver.stop(receiver) end)

    for step <- vector["before_ask"] do
      assert step == "deliver"
      deliver(receiver, answer)
    end

    {receiver, Client.log_decider(receiver)}
  end

  defp deliver(receiver, answer) do
    assert {:ok, _} = Http.post(Receiver.origin(receiver), "/v1/logs", [], answer)
  end

  defp assert_armed(vector) do
    armed = collect_armed([])

    want =
      case vector["expected"]["armed_wait_ns"] do
        nil -> []
        ns -> [div(String.to_integer(ns), 1_000_000)]
      end

    if want != [],
      do: assert(rem(String.to_integer(vector["expected"]["armed_wait_ns"]), 1_000_000) == 0)

    assert armed == want, vector["id"]
  end

  defp collect_armed(acc) do
    receive do
      {:armed, ms} -> collect_armed([ms | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp filter(fixture_filter) do
    specs =
      Enum.map(fixture_filter["specifications"], fn fixture ->
        %SpecificationFilter{
          specification_id: fixture["id"],
          fail_mode:
            if(fixture["fail_mode"] == "closed", do: :FAIL_MODE_CLOSED, else: :FAIL_MODE_OPEN),
          evaluation_mode: :EVALUATION_MODE_ENFORCE,
          readiness: :READINESS_ACTIVE,
          latency_budget_nanoseconds: String.to_integer(fixture["latency_budget_ns"]),
          event_match: %EventMatch{
            event_kinds: fixture["event_kinds"],
            delivery_mode:
              if(fixture["delivery_mode"] == "ask_and_block",
                do: :DELIVERY_MODE_ASK_AND_BLOCK,
                else: :DELIVERY_MODE_SHIP_ASYNC
              )
          }
        }
      end)

    accepted =
      Map.new(fixture_filter["specifications"], fn fixture ->
        {fixture["id"],
         if(fixture["accepted_fail_mode"] == "closed", do: :FAIL_MODE_CLOSED, else: :FAIL_MODE_OPEN)}
      end)

    {%EventFilter{epoch: String.to_integer(fixture_filter["epoch"]), specifications: specs},
     accepted}
  end

  test "decision_wait gate cases run the receiver-backed adapter" do
    gate_cases = Enum.filter(cases(), &(&1["via"] == "gate"))
    assert gate_cases != []

    for vector <- gate_cases do
      {_receiver, decide} = start(vector)
      {event_filter, accepted} = filter(vector["filter"])
      key = {:decision_wait, vector["id"]}
      Process.put({key, :reads}, Enum.map(vector["clock_reads_ns"], &String.to_integer/1))
      Process.put({key, :requests}, [])

      clock = fn ->
        case Process.get({key, :reads}) do
          [value | rest] ->
            Process.put({key, :reads}, rest)
            value

          [] ->
            raise "#{vector["id"]}: clock script exhausted"
        end
      end

      counted = fn request ->
        Process.put({key, :requests}, [request | Process.get({key, :requests})])
        decide.(request)
      end

      outcome =
        Gate.gate(
          %ProducerEvent{id: "fixture-event", kind: vector["event"]["kind"]},
          event_filter,
          vector["local_deadline_ns"] && String.to_integer(vector["local_deadline_ns"]),
          %Gate.Deps{
            decide: counted,
            now_monotonic_ns: clock,
            accepted_fail_mode_for: fn spec -> Map.fetch!(accepted, spec.specification_id) end
          },
          %Gate.Options{
            source_handle: "fixture-source",
            request_id: vector["request_id"],
            idempotency_key: "fixture-idempotency"
          }
        )

      expected = vector["expected"]
      assert Gate.kind_to_string(outcome.kind) == expected["kind"], vector["id"]
      requests = Process.get({key, :requests})
      assert length(requests) == expected["decide_calls"], vector["id"]
      assert Process.get({key, :reads}) == [], vector["id"]

      assert hd(requests).remaining_transport_budget_nanoseconds ==
               String.to_integer(expected["remaining_transport_budget_ns"]),
             vector["id"]

      assert_armed(vector)
    end
  end

  test "decision_wait adapter cases run against the receiver" do
    adapter_cases = Enum.filter(cases(), &(&1["via"] == "adapter"))
    assert adapter_cases != []

    for vector <- adapter_cases do
      {_receiver, decide} = start(vector)
      budget = vector["remaining_transport_budget_ns"]

      request = %DecideRequest{
        request_id: vector["request_id"],
        remaining_transport_budget_nanoseconds: budget && String.to_integer(budget)
      }

      result =
        case decide.(request) do
          {:ok, %{action: :DECISION_ACTION_PERMIT}} -> "permit"
          {:ok, %{action: :DECISION_ACTION_DENY}} -> "deny"
          {:ok, _} -> "unexpected"
          {:error, _} -> "error"
        end

      assert result == vector["expected"]["result"], vector["id"]
      assert_armed(vector)
    end
  end
end
