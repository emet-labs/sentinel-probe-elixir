defmodule Sentinel.Probe.SDK.OtlpGoldenTest do
  use ExUnit.Case, async: true

  alias Sentinel.Model.V1.{EventFilter, ProducerCompletenessSignal, Verdict}
  alias Sentinel.Probe.SDK.Int128
  alias Sentinel.Probe.SDK.Otlp.Codec
  alias Sentinel.Probe.V1.DecideResponse

  @dir Path.expand("../testdata/otlp", __DIR__)

  test "charge-ship golden carries the contract attributes" do
    {:ok, events} = Codec.decode_traces(File.read!(Path.join(@dir, "charge-ship.bin")))
    assert [charge, ship] = events

    assert charge.kind == "sagashop.payment.charge.completed"
    assert charge.id == "01f0c0ffee-charge"
    assert charge.causal_predecessor_ids == []
    assert charge.schema_version == "sagashop-contract-v1"
    assert charge.sequence.epoch == 7
    assert charge.sequence.sequence == 1
    assert charge.acknowledged_filter_epoch == 4
    assert charge.claimed_sensitivity == :SENSITIVITY_INTERNAL
    assert charge.occurrence_time.clock_domain_id == "unix"
    assert charge.occurrence_time.uncertainty_nanoseconds == 0
    assert attribute(charge, "sagashop.payment.status") == "success"
    assert attribute(charge, "sagashop.order.id") == "order-1001"
    refute Enum.any?(charge.causal_predecessor_ids, &span_id?/1)

    assert ship.kind == "sagashop.order.ship.requested"
    assert ship.id == "01f0c0ffee-ship"
    assert ship.causal_predecessor_ids == ["01f0c0ffee-charge"]
    assert ship.sequence.sequence == 2
    assert ship.acknowledged_filter_epoch == 4
    assert attribute(ship, "sagashop.order.id") == "order-1001"
    assert ship.id != ship.causal_predecessor_ids |> hd()
  end

  test "completeness golden is the producer signal" do
    {:ok, [record]} =
      Codec.decode_log_records(File.read!(Path.join(@dir, "completeness-order.bin")))

    assert record.payload_type == Codec.completeness_type()
    assert record.source_handle == "sagashop-order"
    assert record.request_id == nil

    signal = Protobuf.decode(record.body, ProducerCompletenessSignal)
    assert signal.sequence_epoch == 7
    assert signal.last_emitted == 2
    assert signal.complete_through == 2
    assert signal.acknowledged_filter_epoch == 4
    assert signal.activity == :SOURCE_ACTIVITY_STATE_ACTIVE
    assert signal.producer_reported_at.clock_domain_id == "unix"
    assert Int128.to_int(signal.producer_reported_at.nanoseconds) == 1_700_000_000_000_000_000
  end

  test "outbound goldens decode as filter, verdict, and decision" do
    {:ok, [{:event_filter, filter}]} =
      Codec.decode_probe_logs(File.read!(Path.join(@dir, "event-filter.bin")))

    assert %EventFilter{} = filter

    {:ok, [{:verdict, verdict}]} =
      Codec.decode_probe_logs(File.read!(Path.join(@dir, "verdict.bin")))

    assert %Verdict{} = verdict
    assert verdict.outcome != :VERDICT_OUTCOME_UNSPECIFIED

    {:ok, [{:decide, response}]} =
      Codec.decode_probe_logs(File.read!(Path.join(@dir, "decide-response.bin")))

    assert %DecideResponse{} = response
    assert response.request_id != ""

    assert response.action in [
             :DECISION_ACTION_PERMIT,
             :DECISION_ACTION_DENY,
             :DECISION_ACTION_DEFER
           ]
  end

  defp attribute(event, key) do
    Enum.find_value(event.attributes, fn entry ->
      if entry.key == key, do: elem(entry.value.value, 1)
    end)
  end

  defp span_id?(id), do: byte_size(id) == 16 and String.match?(id, ~r/\A[0-9a-f]+\z/)
end
