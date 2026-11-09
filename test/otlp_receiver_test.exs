defmodule Sentinel.Probe.SDK.OtlpReceiverTest do
  use ExUnit.Case, async: true

  import Sentinel.Probe.SDK.TestHelpers

  alias Sentinel.Probe.SDK.Client.Client
  alias Sentinel.Probe.SDK.Enforcement.Gate
  alias Sentinel.Probe.SDK.Otlp.{Codec, Http, Receiver, Wire}
  alias Sentinel.Probe.V1.DecideResponse

  setup do
    {:ok, receiver} = Receiver.start_link()
    Process.unlink(receiver)

    on_exit(fn ->
      if Process.alive?(receiver) do
        Receiver.stop(receiver)
      end
    end)

    %{receiver: receiver}
  end

  test "the gate permits, denies, and defers from OTLP logs", %{receiver: receiver} do
    for action <- [:DECISION_ACTION_PERMIT, :DECISION_ACTION_DENY, :DECISION_ACTION_DEFER] do
      request_id = Atom.to_string(action)
      post_decision!(receiver, request_id, action)

      outcome =
        Gate.gate(
          make_event(test_kind()),
          make_filter(test_epoch(), [ask_and_block_spec()]),
          10_000,
          deps(receiver),
          options(request_id)
        )

      assert outcome.kind == expected_kind(action)
      assert outcome.reason in ["permit", "deny", "defer"]
    end
  end

  test "a missing decision log applies the timeout fail mode", %{receiver: receiver} do
    outcome =
      Gate.gate(
        make_event(test_kind()),
        make_filter(test_epoch(), [ask_and_block_spec()]),
        10_000,
        deps(receiver),
        options("missing")
      )

    assert outcome.kind == :fail_open_permit
    assert String.starts_with?(outcome.reason, "context-deadline-exceeded")
  end

  test "unspecified action is an error and is not a permit", %{receiver: receiver} do
    response = %DecideResponse{request_id: "req-unspec", action: :DECISION_ACTION_UNSPECIFIED}
    bytes = wrap(Protobuf.encode(response), Codec.decide_type(), "req-unspec")
    assert {:error, {:http_status, 400, _}} = post(receiver, bytes)
    assert Receiver.await_decision(receiver, "req-unspec", 0) == {:error, :timeout}
  end

  test "gzip and identity both deliver a decision", %{receiver: receiver} do
    {:ok, plain} =
      Codec.encode_decide(%DecideResponse{request_id: "req-gz", action: :DECISION_ACTION_DENY})

    gzipped = :zlib.gzip(plain)

    assert {:ok, _} =
             Http.post(
               Receiver.origin(receiver),
               "/v1/logs",
               [{"content-encoding", "gzip"}],
               gzipped
             )

    assert {:ok, response} = Receiver.await_decision(receiver, "req-gz", 0)
    assert response.action == :DECISION_ACTION_DENY
  end

  test "a golden event filter replaces the filter store", %{receiver: receiver} do
    config = %Client.Config{source_handle: "sagashop-order", sentinel_base_url: "http://127.0.0.1"}
    client = Client.new(config, receiver)
    bytes = File.read!(Path.expand("../../../testdata/otlp/event-filter.bin", __DIR__))
    assert {:ok, _} = post(receiver, bytes)
    assert [%_{} = filter] = Receiver.filters(receiver)
    assert Client.current_filter(client) == filter
  end

  test "a golden verdict is observed", %{receiver: receiver} do
    bytes = File.read!(Path.expand("../../../testdata/otlp/verdict.bin", __DIR__))
    assert {:ok, _} = post(receiver, bytes)
    assert [%_{outcome: outcome}] = Receiver.verdicts(receiver)
    assert outcome != :VERDICT_OUTCOME_UNSPECIFIED
  end

  test "a non-bytes body is rejected", %{receiver: receiver} do
    record = %Wire.LogRecord{
      body: %Wire.AnyValue{value: {:string_value, "not-bytes"}},
      attributes: [
        %Wire.KeyValue{
          key: "sentinel.payload.type",
          value: %Wire.AnyValue{value: {:string_value, Codec.verdict_type()}}
        }
      ]
    }

    request = %Wire.ExportLogsServiceRequest{
      resource_logs: [%Wire.ResourceLogs{scope_logs: [%Wire.ScopeLogs{log_records: [record]}]}]
    }

    assert {:error, {:http_status, 400, _}} =
             post(receiver, IO.iodata_to_binary(Protobuf.encode(request)))

    assert Receiver.verdicts(receiver) == []
  end

  defp post_decision!(receiver, request_id, action) do
    {:ok, bytes} = Codec.encode_decide(%DecideResponse{request_id: request_id, action: action})
    assert {:ok, _} = post(receiver, bytes)
  end

  defp post(receiver, body) do
    Http.post(Receiver.origin(receiver), "/v1/logs", [], body)
  end

  defp wrap(body, payload_type, request_id) do
    body = IO.iodata_to_binary(body)

    attributes = [
      kv("sentinel.payload.type", payload_type),
      kv("sentinel.request_id", request_id)
    ]

    request = %Wire.ExportLogsServiceRequest{
      resource_logs: [
        %Wire.ResourceLogs{
          scope_logs: [
            %Wire.ScopeLogs{
              log_records: [
                %Wire.LogRecord{
                  body: %Wire.AnyValue{value: {:bytes_value, body}},
                  attributes: attributes
                }
              ]
            }
          ]
        }
      ]
    }

    IO.iodata_to_binary(Protobuf.encode(request))
  end

  defp kv(key, text) do
    %Wire.KeyValue{key: key, value: %Wire.AnyValue{value: {:string_value, text}}}
  end

  defp deps(receiver) do
    %Gate.Deps{
      decide: Gate.decide_from_logs(receiver),
      now_monotonic_ns: fn -> 0 end,
      accepted_fail_mode_for: fn _ -> :FAIL_MODE_OPEN end
    }
  end

  defp options(request_id) do
    %Gate.Options{source_handle: "sagashop-order", request_id: request_id, idempotency_key: "idem"}
  end

  defp expected_kind(:DECISION_ACTION_PERMIT), do: :permit
  defp expected_kind(:DECISION_ACTION_DENY), do: :deny
  defp expected_kind(:DECISION_ACTION_DEFER), do: :defer
end
