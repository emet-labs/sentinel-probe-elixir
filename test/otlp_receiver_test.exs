defmodule Sentinel.Probe.SDK.OtlpReceiverTest do
  use ExUnit.Case, async: true

  import Sentinel.Probe.SDK.TestHelpers

  alias Sentinel.Model.V1.EventFilter
  alias Sentinel.Probe.SDK.Client.{Client, FilterStore}
  alias Sentinel.Probe.SDK.Enforcement.Gate
  alias Sentinel.Probe.SDK.Otlp.{Codec, Http, Receiver, Wire}
  alias Sentinel.Probe.V1.{DecideRequest, DecideResponse}

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
    bytes = File.read!(Path.expand("../testdata/otlp/event-filter.bin", __DIR__))
    assert {:ok, _} = post(receiver, bytes)
    assert [%_{} = filter] = Receiver.filters(receiver)
    applied(client.store)
    assert Client.current_filter(client) == filter
  end

  test "a golden verdict is observed", %{receiver: receiver} do
    bytes = File.read!(Path.expand("../testdata/otlp/verdict.bin", __DIR__))
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

  describe "decision wait through an injected timer" do
    # Each arm reports its wait to the test process and runs `on_arm`, which may
    # deliver a log or expire the wait. Nothing sleeps and no wall clock is read.
    test "an answer that arrives after the ask is returned" do
      receiver =
        scripted_receiver(fn receiver, _expire ->
          post_decision!(receiver, "req-late", :DECISION_ACTION_DENY)
        end)

      assert {:ok, %DecideResponse{action: :DECISION_ACTION_DENY}} =
               ask(receiver, "req-late", 40_000_000)

      assert_received {:armed, 40}
      assert_received :disarmed
    end

    test "a stored answer wins without arming a wait" do
      receiver = scripted_receiver(fn _receiver, expire -> expire.() end)
      post_decision!(receiver, "req-early", :DECISION_ACTION_PERMIT)
      assert {:ok, _} = ask(receiver, "req-early", 40_000_000)
      refute_received {:armed, _}
    end

    test "an absent or zero budget does not wait" do
      receiver = scripted_receiver(fn _receiver, expire -> expire.() end)
      assert {:error, _} = ask(receiver, "req-none", nil)
      assert {:error, _} = ask(receiver, "req-zero", 0)
      refute_received {:armed, _}
    end

    test "expiry fails the ask" do
      receiver = scripted_receiver(fn _receiver, expire -> expire.() end)
      assert {:error, %{kind: :deadline}} = ask(receiver, "req-expired", 40_000_000)
      assert_received {:armed, 40}
    end

    test "an answer for another request does not answer the ask" do
      receiver =
        scripted_receiver(fn receiver, expire ->
          post_decision!(receiver, "req-other", :DECISION_ACTION_DENY)
          expire.()
        end)

      assert {:error, %{kind: :deadline}} = ask(receiver, "req-mine", 40_000_000)
    end
  end

  test "attaching a store replays received Filters and keeps the highest epoch", %{
    receiver: receiver
  } do
    for epoch <- [2, 1] do
      assert {:ok, _} = post(receiver, filter_log(epoch))
    end

    config = %Client.Config{
      source_handle: "seed-#{inspect(self())}",
      sentinel_base_url: "http://127.0.0.1"
    }

    client = Client.new(config, receiver)
    applied(client.store)
    assert Client.acknowledged_epoch(client) == 2
    FilterStore.delete(client.store)
  end

  # Contention: the store's writer is held suspended, which is the worst case of
  # a Filter apply that has not finished. Deterministic interleaving, no clock.
  test "a pending Filter apply blocks neither decision registration nor delivery" do
    receiver =
      scripted_receiver(fn receiver, _expire ->
        post_decision!(receiver, "req-contended", :DECISION_ACTION_PERMIT)
      end)

    config = %Client.Config{
      source_handle: "contended-#{inspect(self())}",
      sentinel_base_url: "http://127.0.0.1"
    }

    client = Client.new(config, receiver)
    :ok = :sys.suspend(FilterStore.writer(client.store))

    try do
      # The push is acknowledged while its apply is still queued.
      assert {:ok, _} = post(receiver, filter_log(3))
      assert Client.acknowledged_epoch(client) == nil

      # A wait is registered, then answered from inside the armed timer, while
      # the apply is still queued.
      assert {:ok, %DecideResponse{action: :DECISION_ACTION_PERMIT}} =
               ask(receiver, "req-contended", 40_000_000)

      assert_received {:armed, 40}
    after
      :ok = :sys.resume(FilterStore.writer(client.store))
    end

    applied(client.store)
    assert Client.acknowledged_epoch(client) == 3
    FilterStore.delete(client.store)
  end

  # A writer killed with a pushed Filter still in its mailbox loses that
  # message. The receiver monitors the writer and replays every Filter it has
  # received, so the store still ends on the pushed epoch.
  test "the receiver replays Filters when the store's writer dies", %{receiver: receiver} do
    config = %Client.Config{
      source_handle: "killed-writer-#{inspect(self())}",
      sentinel_base_url: "http://127.0.0.1"
    }

    client = Client.new(config, receiver)
    writer = FilterStore.writer(client.store)
    :ok = :sys.suspend(writer)
    assert {:ok, _} = post(receiver, filter_log(4))
    ref = Process.monitor(writer)
    Process.exit(writer, :kill)
    assert_receive {:DOWN, ^ref, :process, ^writer, :killed}
    assert Client.acknowledged_epoch(client) == nil

    # The receiver handles its own :DOWN in mailbox order; a call issued now is
    # answered only after any :DOWN already delivered to it. Spin, with no
    # clock, until the replay has reached the restarted writer.
    assert await_epoch(receiver, client, 4, 10_000)
    refute FilterStore.writer(client.store) == writer
    FilterStore.delete(client.store)
  end

  # The receiver sees the deleted writer go :DOWN and must detach, not replay;
  # a later push must not restart the writer either. Both orders of the push
  # and the :DOWN leave the store deleted.
  test "deleting an attached store does not bring it back", %{receiver: receiver} do
    config = %Client.Config{
      source_handle: "deleted-store-#{inspect(self())}",
      sentinel_base_url: "http://127.0.0.1"
    }

    client = Client.new(config, receiver)
    assert {:ok, _} = post(receiver, filter_log(2))
    applied(client.store)
    assert Client.acknowledged_epoch(client) == 2

    writer = FilterStore.writer(client.store)
    ref = Process.monitor(writer)
    :ok = FilterStore.delete(client.store)
    assert_receive {:DOWN, ^ref, :process, ^writer, _}

    assert {:ok, _} = post(receiver, filter_log(5))
    _ = :sys.get_state(receiver)

    assert FilterStore.deleted?(client.store)
    assert Client.acknowledged_epoch(client) == nil
    assert FilterStore.running_writer(client.store) == nil
    assert Client.set_filter(client, %EventFilter{epoch: 9}) == false
    assert FilterStore.running_writer(client.store) == nil
  end

  defp await_epoch(_receiver, _client, _epoch, 0), do: false

  defp await_epoch(receiver, client, epoch, attempts) do
    _ = Receiver.filters(receiver)

    case FilterStore.running_writer(client.store) do
      nil -> :ok
      pid -> :sys.get_state(pid)
    end

    if Client.acknowledged_epoch(client) == epoch,
      do: true,
      else: await_epoch(receiver, client, epoch, attempts - 1)
  end

  # Wait until the store's writer has applied every Filter handed to it so far.
  # `:sys.get_state/1` is answered after the writer's earlier mailbox messages.
  defp applied(store), do: :sys.get_state(FilterStore.writer(store))

  defp scripted_receiver(on_arm) do
    test_pid = self()

    # The timer runs in the asking process, which here is the test process.
    timer = fn timeout_ms, ref ->
      send(test_pid, {:armed, timeout_ms})
      on_arm.(Process.get(:scripted_receiver), fn -> send(self(), {:decision_timeout, ref}) end)
      fn -> send(test_pid, :disarmed) end
    end

    {:ok, receiver} = Receiver.start_link(decision_timer: timer)
    Process.unlink(receiver)
    Process.put(:scripted_receiver, receiver)
    on_exit(fn -> if Process.alive?(receiver), do: Receiver.stop(receiver) end)
    receiver
  end

  defp ask(receiver, request_id, budget_ns) do
    Gate.decide_from_logs(receiver).(%DecideRequest{
      request_id: request_id,
      remaining_transport_budget_nanoseconds: budget_ns
    })
  end

  defp filter_log(epoch) do
    body = IO.iodata_to_binary(Protobuf.encode(%EventFilter{epoch: epoch}))

    request = %Wire.ExportLogsServiceRequest{
      resource_logs: [
        %Wire.ResourceLogs{
          scope_logs: [
            %Wire.ScopeLogs{
              log_records: [
                %Wire.LogRecord{
                  body: %Wire.AnyValue{value: {:bytes_value, body}},
                  attributes: [kv("sentinel.payload.type", Codec.event_filter_type())]
                }
              ]
            }
          ]
        }
      ]
    }

    IO.iodata_to_binary(Protobuf.encode(request))
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
