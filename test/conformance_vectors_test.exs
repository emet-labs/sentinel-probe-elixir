defmodule Sentinel.Probe.SDK.ConformanceVectorsTest do
  use ExUnit.Case, async: true

  alias Sentinel.Model.V1.{EventFilter, EventMatch, Int128, ProducerEvent, SpecificationFilter}
  alias Sentinel.Probe.V1.DecideResponse
  alias Sentinel.Probe.SDK.Enforcement.Gate
  alias Sentinel.Probe.SDK.Int128, as: Int128Codec
  alias Sentinel.Probe.SDK.Internal.SpecMatch

  @root Path.expand("../../../testdata/probe-sdk-conformance", __DIR__)

  defp load(name) do
    document = @root |> Path.join(name) |> File.read!() |> Jason.decode!()
    assert Map.keys(document) |> Enum.sort() == ["cases", "format_version", "kind"]
    assert document["format_version"] == "1.0.0"
    document
  end

  test "manifest suite registry fails closed" do
    manifest = @root |> Path.join("manifest-v1.json") |> File.read!() |> Jason.decode!()
    assert Enum.map(manifest["suites"], & &1["kind"]) == ["spec_match", "int128", "enforcement_gate"]
  end

  test "exact Int128 words and independent decoding follow shared vectors" do
    suite = load("int128-v1.json")
    assert suite["kind"] == "int128"
    assert Enum.uniq_by(suite["cases"], & &1["id"]) == suite["cases"]

    for vector <- suite["cases"] do
      {value, ""} = Integer.parse(vector["value"])
      {high, ""} = Integer.parse(vector["high"])
      {low, ""} = Integer.parse(vector["low"])
      encoded = Int128Codec.from_int(value)
      assert encoded.high == high, vector["id"]
      assert encoded.low == low, vector["id"]
      assert Int128Codec.to_int(%Int128{high: high, low: low}) == value, vector["id"]
    end
  end

  test "SpecMatch follows shared vectors" do
    suite = load("spec-match-v1.json")
    assert suite["kind"] == "spec_match"
    assert Enum.uniq_by(suite["cases"], & &1["id"]) == suite["cases"]

    for vector <- suite["cases"] do
      fixture_match = vector["specification_filter"]["event_match"]

      event_match =
        if fixture_match do
          %EventMatch{
            event_kinds: fixture_match["event_kinds"],
            projected_attribute_keys: fixture_match["projected_attribute_keys"]
          }
        end

      spec = %SpecificationFilter{event_match: event_match}
      event = %ProducerEvent{kind: vector["producer_event"]["kind"]}
      assert SpecMatch.selects?(spec, event) == vector["expected"], vector["id"]
    end
  end

  test "enforcement scenarios execute the production gate" do
    suite = load("enforcement-gate-v1.json")

    actions = %{
      "permit" => :DECISION_ACTION_PERMIT,
      "deny" => :DECISION_ACTION_DENY,
      "defer" => :DECISION_ACTION_DEFER,
      "unspecified" => :DECISION_ACTION_UNSPECIFIED
    }

    for vector <- suite["cases"] do
      accepted =
        Map.new(vector["filter"] && vector["filter"]["specifications"] || [], fn fixture ->
          mode = if fixture["accepted_fail_mode"] == "closed", do: :FAIL_MODE_CLOSED, else: :FAIL_MODE_OPEN
          {fixture["id"], mode}
        end)

      filter =
        if vector["filter"] do
          specs =
            Enum.map(vector["filter"]["specifications"], fn fixture ->
              %SpecificationFilter{
                specification_id: fixture["id"],
                fail_mode: if(fixture["fail_mode"] == "closed", do: :FAIL_MODE_CLOSED, else: :FAIL_MODE_OPEN),
                evaluation_mode: :EVALUATION_MODE_ENFORCE,
                readiness: :READINESS_ACTIVE,
                latency_budget_nanoseconds: String.to_integer(fixture["latency_budget_ns"]),
                event_match: %EventMatch{
                  event_kinds: fixture["event_kinds"],
                  delivery_mode: if(fixture["delivery_mode"] == "ask_and_block", do: :DELIVERY_MODE_ASK_AND_BLOCK, else: :DELIVERY_MODE_SHIP_ASYNC)
                }
              }
            end)

          %EventFilter{epoch: String.to_integer(vector["filter"]["epoch"]), specifications: specs}
        end

      key = {:conformance, vector["id"]}
      Process.put({key, :reads}, Enum.map(vector["clock_reads_ns"], &String.to_integer/1))
      Process.put({key, :read_count}, 0)
      Process.put({key, :requests}, [])

      clock = fn ->
        case Process.get({key, :reads}) do
          [value | rest] ->
            Process.put({key, :reads}, rest)
            Process.put({key, :read_count}, Process.get({key, :read_count}) + 1)
            value

          [] ->
            raise "clock script exhausted"
        end
      end

      decide = fn request ->
        Process.put({key, :requests}, [request | Process.get({key, :requests})])
        result = vector["decider"]["result"]
        if result == "transport_error", do: {:error, "fixture-transport-error"}, else: {:ok, %DecideResponse{action: actions[result]}}
      end

      outcome =
        Gate.gate(
          %ProducerEvent{id: "fixture-event", kind: vector["event"]["kind"]},
          filter,
          vector["local_deadline_ns"] && String.to_integer(vector["local_deadline_ns"]),
          %Gate.Deps{
            decide: decide,
            now_monotonic_ns: clock,
            accepted_fail_mode_for: fn spec -> Map.fetch!(accepted, spec.specification_id) end
          },
          %Gate.Options{source_handle: "fixture-source", request_id: "fixture-request", idempotency_key: "fixture-idempotency"}
        )

      assert Gate.kind_to_string(outcome.kind) == vector["expected"]["kind"], vector["id"]
      assert length(Process.get({key, :requests})) == vector["expected"]["decide_calls"], vector["id"]
      assert Process.get({key, :read_count}) == length(vector["clock_reads_ns"]), vector["id"]
      assert Process.get({key, :reads}) == []
    end
  end
end
