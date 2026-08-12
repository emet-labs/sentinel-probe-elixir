defmodule Sentinel.Probe.SDK.TransportTest do
  use ExUnit.Case, async: true
  alias Sentinel.Probe.SDK.Client.Transport
  alias Sentinel.Probe.V1.{DecideRequest, DecideResponse}

  test "path is the Connect unary path" do
    assert Transport.path() == "/sentinel.probe.v1.SentinelDecisionService/Decide"
  end

  test "encode_request produces raw protobuf bytes" do
    req = %DecideRequest{request_id: "r1", source_handle: "s"}
    bytes = Transport.encode_request(req)
    assert is_binary(bytes)
    assert byte_size(bytes) > 0
  end

  test "decode_response round-trips through encode" do
    req = %DecideRequest{request_id: "r1", source_handle: "s", filter_epoch: 5}
    bytes = Transport.encode_request(req)
    assert {:ok, %DecideRequest{}} = {:ok, Protobuf.decode(bytes, DecideRequest)}
  end

  test "parse_error parses a Connect error envelope" do
    body = ~s({"code": "unavailable", "message": "endpoint down"})
    error = Transport.parse_error(503, body)
    assert error.kind == :connect
    assert error.code == :unavailable
    assert error.message == "endpoint down"
  end

  test "parse_error with missing code still reads message" do
    body = ~s({"message": "unknown"})
    error = Transport.parse_error(500, body)
    assert error.kind == :connect
  end

  test "parse_error falls back to transport for unparseable body" do
    error = Transport.parse_error(500, "garbage")
    assert error.kind == :transport
  end

  test "describe_error classifies deadline" do
    alias Sentinel.Probe.SDK.Enforcement.Gate
    error = %Transport.Error{kind: :deadline, message: "timeout"}
    assert String.starts_with?(Gate.describe_error(error), "context-deadline-exceeded")
  end

  test "describe_error classifies connect" do
    alias Sentinel.Probe.SDK.Enforcement.Gate
    error = %Transport.Error{kind: :connect, code: :unavailable, message: "down"}
    assert String.starts_with?(Gate.describe_error(error), "connect-unavailable")
  end
end
