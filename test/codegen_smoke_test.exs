defmodule Sentinel.Probe.SDK.CodegenSmokeTest do
  use ExUnit.Case, async: true

  test "generated model modules exist and compile" do
    assert Code.ensure_loaded?(Sentinel.Model.V1.ProducerEvent)
    assert Code.ensure_loaded?(Sentinel.Model.V1.EventFilter)
    assert Code.ensure_loaded?(Sentinel.Model.V1.SpecificationFilter)
    assert Code.ensure_loaded?(Sentinel.Model.V1.AttributeValue)
    assert Code.ensure_loaded?(Sentinel.Model.V1.Int128)
  end

  test "generated probe modules exist and compile" do
    assert Code.ensure_loaded?(Sentinel.Probe.V1.DecideRequest)
    assert Code.ensure_loaded?(Sentinel.Probe.V1.DecideResponse)
    assert Code.ensure_loaded?(Sentinel.Probe.V1.SpecificationDecision)
  end

  test "enums are generated with their atoms" do
    assert Sentinel.Model.V1.FailMode.value(:FAIL_MODE_CLOSED) == 2
    assert Sentinel.Probe.V1.DecisionAction.value(:DECISION_ACTION_PERMIT) == 1
  end

  test "protobuf roundtrip through DecideRequest" do
    alias Sentinel.Probe.V1.DecideRequest
    req = %DecideRequest{request_id: "r1", source_handle: "s", filter_epoch: 7}
    bytes = IO.iodata_to_binary(Protobuf.encode(req))
    dec = Protobuf.decode(bytes, DecideRequest)
    assert dec.request_id == "r1"
    assert dec.filter_epoch == 7
  end
end
