defmodule Sentinel.Probe.SDK.ClientTest do
  use ExUnit.Case, async: true

  alias Sentinel.Model.V1.{EventFilter, ProducerEvent}
  alias Sentinel.Probe.SDK.Client.Client
  alias Sentinel.Probe.V1.{DecideResponse, SpecificationDecision}

  defp key(n), do: {:sentinel_test_client, n, self()}

  defp config(initial \\ nil) do
    %Client.Config{
      source_handle: "test.source",
      sentinel_base_url: "http://localhost:7070",
      initial_filter: initial
    }
  end

  defp new_client(initial \\ nil, decider \\ fn _req -> {:ok, %DecideResponse{}} end) do
    # Use a unique persistent_term key per test to avoid interference.
    %Client.Config{config(initial) | source_handle: "test.#{inspect(self())}"}
    Client.new(config(initial), decider)
  end

  test "current_filter is nil before the first set" do
    c = new_client()
    assert Client.current_filter(c) == nil
  end

  test "set_filter swaps in a new filter" do
    c = new_client()
    f = %EventFilter{epoch: 1}
    assert Client.set_filter(c, f) == true
    assert Client.current_filter(c) == f
  end

  test "acknowledged_epoch returns nil before the first set" do
    c = new_client()
    assert Client.acknowledged_epoch(c) == nil
  end

  test "acknowledged_epoch returns the held epoch" do
    c = new_client(%EventFilter{epoch: 7})
    assert Client.acknowledged_epoch(c) == 7
  end

  test "refresh_on_epoch: no filter means refresh" do
    c = new_client()
    assert Client.refresh_on_epoch(c, 1) == true
  end

  test "refresh_on_epoch: same epoch means no refresh" do
    c = new_client(%EventFilter{epoch: 5})
    assert Client.refresh_on_epoch(c, 5) == false
  end

  test "refresh_on_epoch: nil means no refresh" do
    c = new_client(%EventFilter{epoch: 5})
    assert Client.refresh_on_epoch(c, nil) == false
  end

  test "source_handle returns the configured handle" do
    c = new_client()
    assert String.starts_with?(Client.source_handle(c), "test.")
  end

  test "build_decide_request stamps the source handle and epoch" do
    c = new_client(%EventFilter{epoch: 42})
    event = %ProducerEvent{id: "e1", kind: "k"}
    req = Client.build_decide_request(c, event, "req-1", "idem-1", 5000)
    assert req.request_id == "req-1"
    assert req.idempotency_key == "idem-1"
    assert req.filter_epoch == 42
    assert req.producer_event == event
    assert req.remaining_transport_budget_nanoseconds == 5000
  end

  test "decide_func returns the injected decider" do
    ref = make_ref()
    decider = fn _req -> {:ok, %DecideResponse{request_id: "x"}} end
    c = new_client(nil, decider)
    assert Client.decide_func(c) == decider
  end

  test "transport_decider builds a decider function" do
    f = Client.transport_decider("http://localhost:7070", :unused_finch)
    assert is_function(f, 1)
  end
end
