defmodule Sentinel.Probe.SDK.MockDecideTest do
  use ExUnit.Case, async: true
  alias Sentinel.Probe.SDK.MockDecider
  alias Sentinel.Probe.V1.{DecideRequest, DecideResponse}

  setup do
    MockDecider.reset!()
    :ok
  end

  test "returns the configured response" do
    mock = MockDecider.new(response: %DecideResponse{request_id: "r1"})
    assert {:ok, resp} = MockDecider.decide(mock, %DecideRequest{request_id: "r1"})
    assert resp.request_id == "r1"
  end

  test "returns the configured error" do
    mock = MockDecider.new(error: "boom")
    assert {:error, "boom"} = MockDecider.decide(mock, %DecideRequest{})
  end

  test "counts calls" do
    mock = MockDecider.new(response: %DecideResponse{})
    MockDecider.decide(mock, %DecideRequest{})
    MockDecider.decide(mock, %DecideRequest{})
    assert MockDecider.call_count() == 2
  end

  test "records the last request" do
    mock = MockDecider.new(response: %DecideResponse{})
    MockDecider.decide(mock, %DecideRequest{request_id: "last"})
    assert MockDecider.last_request().request_id == "last"
  end

  test "reset clears call state" do
    mock = MockDecider.new(response: %DecideResponse{})
    MockDecider.decide(mock, %DecideRequest{})
    MockDecider.reset!()
    assert MockDecider.call_count() == 0
    assert MockDecider.last_request() == nil
  end
end
