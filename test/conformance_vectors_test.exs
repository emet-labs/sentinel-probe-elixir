defmodule Sentinel.Probe.SDK.ConformanceVectorsTest do
  use ExUnit.Case, async: true

  alias Sentinel.Model.V1.{
    EventFilter,
    EventMatch,
    Int128,
    ProducerCompletenessSignal,
    ProducerEvent,
    SpecificationFilter,
    Verdict
  }

  alias Sentinel.Probe.SDK.Otlp.Codec
  alias Sentinel.Probe.V1.DecideResponse
  alias Sentinel.Probe.SDK.Enforcement.Gate
  alias Sentinel.Probe.SDK.Int128, as: Int128Codec
  alias Sentinel.Probe.SDK.Internal.SpecMatch

  @root Path.expand("../testdata/probe-sdk-conformance", __DIR__)

  defp load(name, version \\ "1.0.0") do
    document = @root |> Path.join(name) |> File.read!() |> Jason.decode!()
    assert Map.keys(document) |> Enum.sort() == ["cases", "format_version", "kind"]
    assert document["format_version"] == version
    document
  end

  test "manifest suite registry fails closed" do
    manifest = @root |> Path.join("manifest-v1.json") |> File.read!() |> Jason.decode!()

    assert manifest["format_version"] == "1.4.0"

    assert Enum.map(manifest["suites"], & &1["kind"]) == [
             "spec_match",
             "int128",
             "enforcement_gate",
             "otlp_signal",
             "sequence_completeness",
             "decision_wait",
             "filter_store"
           ]
  end

  test "shared malformed corpus is rejected by category" do
    manifest = @root |> Path.join("manifest-v1.json") |> File.read!() |> Jason.decode!()

    for fixture <- manifest["malformed"] do
      assert malformed_category(fixture["path"]) == fixture["rejection_category"], fixture["path"]
    end
  end

  defp malformed_category(name) do
    raw = @root |> Path.join(name) |> File.read!()

    if name == "malformed/duplicate-key.json" and length(Regex.scan(~r/"kind"\s*:/, raw)) > 1 do
      "duplicate-key"
    else
      value = Jason.decode!(raw)

      cond do
        value["format_version"] != "1.0.0" ->
          "version"

        not Map.has_key?(value, "cases") ->
          "missing-field"

        Enum.any?(Map.keys(value), &(&1 not in ["format_version", "kind", "cases"])) ->
          "unknown-field"

        value["kind"] not in ["spec_match", "int128", "enforcement_gate"] ->
          "unknown-token"

        value["kind"] == "int128" ->
          malformed_int128_category(value["cases"])

        true ->
          "accepted"
      end
    end
  end

  defp malformed_int128_category(cases) do
    ids = Enum.map(cases, & &1["id"])
    decimal = ~r/^(0|-?[1-9][0-9]*)$/

    cond do
      length(Enum.uniq(ids)) != length(ids) ->
        "duplicate-id"

      Enum.any?(cases, fn item ->
        Enum.any?(["value", "high", "low"], &(not (item[&1] =~ decimal)))
      end) ->
        "integer-lexeme"

      Enum.any?(cases, fn item ->
        value = String.to_integer(item["value"])
        value < -Integer.pow(2, 127) or value >= Integer.pow(2, 127)
      end) ->
        "integer-range"

      true ->
        "accepted"
    end
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
        Map.new((vector["filter"] && vector["filter"]["specifications"]) || [], fn fixture ->
          mode =
            if fixture["accepted_fail_mode"] == "closed",
              do: :FAIL_MODE_CLOSED,
              else: :FAIL_MODE_OPEN

          {fixture["id"], mode}
        end)

      filter =
        if vector["filter"] do
          specs =
            Enum.map(vector["filter"]["specifications"], fn fixture ->
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

        if result == "transport_error",
          do: {:error, "fixture-transport-error"},
          else: {:ok, %DecideResponse{action: actions[result]}}
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
          %Gate.Options{
            source_handle: "fixture-source",
            request_id: "fixture-request",
            idempotency_key: "fixture-idempotency"
          }
        )

      assert Gate.kind_to_string(outcome.kind) == vector["expected"]["kind"], vector["id"]

      assert length(Process.get({key, :requests})) == vector["expected"]["decide_calls"],
             vector["id"]

      assert Process.get({key, :read_count}) == length(vector["clock_reads_ns"]), vector["id"]
      assert Process.get({key, :reads}) == []
    end
  end

  test "otlp_signal vectors decode events and messages" do
    suite = load("otlp-signal-v1.json", "1.1.0")
    assert suite["kind"] == "otlp_signal"
    assert Enum.uniq_by(suite["cases"], & &1["id"]) == suite["cases"]

    for vector <- suite["cases"] do
      assert_otlp(vector)
    end
  end

  defp assert_otlp(%{"signal" => "trace", "reject" => true, "path" => path}) do
    assert {:error, _} = Codec.decode_traces(otlp_bytes(path))
  end

  defp assert_otlp(%{"signal" => "trace", "id" => id, "path" => path, "events" => expected}) do
    {:ok, events} = Codec.decode_traces(otlp_bytes(path))
    assert length(events) == length(expected), id

    for {event, want} <- Enum.zip(events, expected) do
      assert event.id == want["id"], id
      assert event.kind == want["kind"], id
      assert event.sequence.epoch == String.to_integer(want["sequence_epoch"]), id
      assert event.sequence.sequence == String.to_integer(want["sequence_number"]), id
      assert event.causal_predecessor_ids == want["predecessors"], id
    end
  end

  defp assert_otlp(%{"signal" => "completeness", "path" => path, "completeness" => want}) do
    {:ok, [record]} = Codec.decode_log_records(otlp_bytes(path))
    assert record.payload_type == Codec.completeness_type()
    assert record.source_handle == want["source_handle"]
    signal = Protobuf.decode(record.body, ProducerCompletenessSignal)
    assert signal.sequence_epoch == String.to_integer(want["sequence_epoch"])
    assert signal.last_emitted == String.to_integer(want["last_emitted"])
    assert signal.complete_through == String.to_integer(want["complete_through"])
    assert signal.acknowledged_filter_epoch == String.to_integer(want["acknowledged_filter_epoch"])
    assert signal.producer_reported_at.clock_domain_id == want["clock_domain_id"]

    assert Int128Codec.to_int(signal.producer_reported_at.nanoseconds) ==
             String.to_integer(want["occurrence_ns"])

    assert signal.producer_reported_at.uncertainty_nanoseconds ==
             String.to_integer(want["uncertainty_nanoseconds"])

    assert signal.activity == activity_atom(want["activity"])
  end

  defp assert_otlp(%{"signal" => "verdict", "path" => path, "verdict" => want}) do
    {:ok, [{:verdict, verdict}]} = Codec.decode_probe_logs(otlp_bytes(path))
    assert %Verdict{} = verdict
    assert verdict.id == want["id"]
    assert verdict.outcome == outcome_atom(want["outcome"])
  end

  defp assert_otlp(%{"signal" => "event_filter", "path" => path, "event_filter" => want}) do
    {:ok, [{:event_filter, filter}]} = Codec.decode_probe_logs(otlp_bytes(path))
    assert %EventFilter{} = filter
    assert filter.epoch == String.to_integer(want["epoch"])
    assert Enum.map(filter.specifications, & &1.specification_id) == want["specification_ids"]
  end

  defp assert_otlp(%{"signal" => "decide_response", "path" => path, "decide_response" => want}) do
    {:ok, [{:decide, response}]} = Codec.decode_probe_logs(otlp_bytes(path))
    assert %DecideResponse{} = response
    assert response.request_id == want["request_id"]
    assert response.action == action_atom(want["action"])

    assert Enum.map(response.specifications, &{&1.specification_id, &1.action}) ==
             Enum.map(want["specifications"], &{&1["id"], action_atom(&1["action"])})
  end

  defp otlp_bytes(relative), do: relative |> Path.expand(@root) |> File.read!()

  defp activity_atom("active"), do: :SOURCE_ACTIVITY_STATE_ACTIVE
  defp activity_atom("idle"), do: :SOURCE_ACTIVITY_STATE_INTENTIONALLY_IDLE
  defp outcome_atom("satisfy"), do: :VERDICT_OUTCOME_SATISFY
  defp outcome_atom("violate"), do: :VERDICT_OUTCOME_VIOLATE
  defp outcome_atom("inconclusive"), do: :VERDICT_OUTCOME_INCONCLUSIVE
  defp action_atom("permit"), do: :DECISION_ACTION_PERMIT
  defp action_atom("deny"), do: :DECISION_ACTION_DENY
  defp action_atom("defer"), do: :DECISION_ACTION_DEFER
end
