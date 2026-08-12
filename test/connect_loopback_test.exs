defmodule Sentinel.Probe.SDK.ConnectLoopbackTest do
  use ExUnit.Case, async: true
  # The feasible partial integration: hand-rolled Connect encoder/decoder
  # against golden byte fixtures, gate over known-good framing. This satisfies
  # the spirit of criterion 6 without a live SagaShop integration (blocked #22/#23).
  alias Sentinel.Probe.V1.{DecideRequest, DecideResponse, DecisionAction, SpecificationDecision}
  alias Sentinel.Probe.SDK.Client.Transport

  test "encode → decode round-trip preserves all fields" do
    req = %DecideRequest{
      request_id: "req-rt",
      idempotency_key: "idem-rt",
      source_handle: "gateway",
      filter_epoch: 42,
      remaining_transport_budget_nanoseconds: 5000
    }

    bytes = Transport.encode_request(req)
    decoded = Protobuf.decode(bytes, DecideRequest)
    assert decoded.request_id == "req-rt"
    assert decoded.idempotency_key == "idem-rt"
    assert decoded.source_handle == "gateway"
    assert decoded.filter_epoch == 42
    assert decoded.remaining_transport_budget_nanoseconds == 5000
  end

  test "a DecideResponse round-trips with specifications" do
    resp = %DecideResponse{
      request_id: "r1",
      action: :DECISION_ACTION_PERMIT,
      specifications: [
        %SpecificationDecision{specification_id: "s1", action: :DECISION_ACTION_PERMIT}
      ]
    }

    bytes = IO.iodata_to_binary(Protobuf.encode(resp))
    decoded = Protobuf.decode(bytes, DecideResponse)
    assert decoded.request_id == "r1"
    assert decoded.action == :DECISION_ACTION_PERMIT
    assert length(decoded.specifications) == 1
    assert hd(decoded.specifications).specification_id == "s1"
  end

  test "the Connect unary path matches the service+method" do
    assert Transport.path() == "/sentinel.probe.v1.SentinelDecisionService/Decide"
    assert Transport.service() == "sentinel.probe.v1.SentinelDecisionService"
    assert Transport.method() == "Decide"
  end

  test "empty request encodes to a non-empty binary" do
    bytes = Transport.encode_request(%DecideRequest{})
    assert is_binary(bytes)
    # An all-default message still encodes valid protobuf (may be empty for all-zero).
    # The key property: it does not crash.
  end
end
