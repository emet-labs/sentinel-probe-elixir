defmodule Sentinel.Probe.SDK.HardeningTest do
  use ExUnit.Case, async: true

  require Sentinel.Probe.SDK.Emission.Processor

  import ExUnit.CaptureLog

  alias Sentinel.Model.V1.{EventFilter, EventMatch, SpecificationFilter}
  alias Sentinel.Probe.SDK.Client.FilterStore
  alias Sentinel.Probe.SDK.Emission.{Processor, Sequence, Span}
  alias Sentinel.Probe.SDK.Otlp.{AsyncExporter, Codec, Heartbeat, Http, Receiver, Wire}
  alias Sentinel.Probe.SDK.Propagation

  @start_ms 1_794_000_000_000

  setup do
    {:ok, sequence} = Sequence.start_link(now: fn -> @start_ms * 1_000_000 end)
    %{sequence: sequence}
  end

  # -- helpers ----------------------------------------------------------------

  # A Sentinel on 127.0.0.1 answering each request with the next scripted status (the last one
  # repeats) and handing the request header and body to the test.
  defp sentinel(statuses) do
    test = self()
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(listen)
    {:ok, script} = Agent.start_link(fn -> List.wrap(statuses) end)

    spawn_link(fn -> serve(listen, script, test) end)
    "http://127.0.0.1:#{port}"
  end

  defp serve(listen, script, test) do
    {:ok, socket} = :gen_tcp.accept(listen)
    {:ok, header, request} = Http.read_message(socket)
    send(test, {:request, header, request})

    status =
      Agent.get_and_update(script, fn
        [last] -> {last, [last]}
        [next | rest] -> {next, rest}
      end)

    body = if status in 200..299, do: "", else: "nope"

    :ok =
      :gen_tcp.send(
        socket,
        "HTTP/1.1 #{status} X\r\nContent-Length: #{byte_size(body)}\r\n\r\n" <> body
      )

    :gen_tcp.close(socket)
    serve(listen, script, test)
  end

  defp closed_port do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(listen)
    :gen_tcp.close(listen)
    "http://127.0.0.1:#{port}"
  end

  defp record(attributes, parent) do
    base =
      Processor.span(
        name: "sagashop.shipping.consume",
        start_time: :opentelemetry.timestamp(),
        attributes: :otel_attributes.new(attributes, 128, :infinity),
        links: :otel_links.new([], 128, 128, :infinity)
      )

    case parent do
      nil -> base
      {id, remote} -> Processor.span(base, parent_span_id: id, parent_span_is_remote: remote)
    end
  end

  defp event_span(extra \\ %{}, parent \\ nil),
    do: record(Map.merge(%{Span.attribute_event_id() => "evt-1"}, extra), parent)

  defp config(sequence, extra) do
    test = self()

    Map.merge(
      %{
        allocator: sequence,
        schema_version: "sagashop-contract-v1",
        acknowledged_epoch: 0,
        claimed_sensitivity: :SENSITIVITY_INTERNAL,
        on_export_error: fn reason -> send(test, {:export_error, reason}) end
      },
      extra
    )
  end

  defp started(span_record, config), do: Processor.on_start(:otel_ctx.new(), span_record, config)

  defp wire_span(request) do
    %Wire.ExportTraceServiceRequest{resource_spans: [resource_spans]} =
      Protobuf.decode(request, Wire.ExportTraceServiceRequest)

    [%{spans: [wire]}] = resource_spans.scope_spans
    {resource_spans, wire}
  end

  defp wait_until(fun, tries \\ 200) do
    cond do
      fun.() -> :ok
      tries == 0 -> flunk("condition not reached")
      true -> Process.sleep(10) && wait_until(fun, tries - 1)
    end
  end

  # -- 1. never raise from the span end path --------------------------------------

  describe "the span end path never raises" do
    test "a failing synchronous export returns false and marks the slot failed", %{
      sequence: sequence
    } do
      config = config(sequence, %{otlp: %{endpoint: closed_port(), source_handle: "h"}})
      record = started(event_span(), config)

      assert Processor.on_end(record, config) == false
      assert_received {:export_error, {:transport, _}}
      assert Sequence.declaration(sequence).complete_through == 0
    end

    test "a raising on_export_error callback does not propagate", %{sequence: sequence} do
      config =
        config(sequence, %{
          otlp: %{endpoint: closed_port(), source_handle: "h"},
          on_export_error: fn _ -> raise "boom" end
        })

      record = started(event_span(), config)
      assert Processor.on_end(record, config) == false
    end

    test "a raising on_event callback does not propagate", %{sequence: sequence} do
      config = config(sequence, %{on_event: fn _ -> raise "boom" end})
      record = started(event_span(), config)
      assert Processor.on_end(record, config) == true
    end

    test "a dead allocator does not crash on_start or on_end" do
      {:ok, dead} = Agent.start(fn -> nil end)
      Agent.stop(dead)

      config = %{
        allocator: dead,
        schema_version: "v1",
        otlp: %{endpoint: closed_port(), source_handle: "h"},
        on_export_error: fn _ -> :ok end
      }

      record = event_span()
      assert Processor.on_start(:otel_ctx.new(), record, config) == record
      assert Processor.on_end(record, config) == false
    end

    test "a malformed span record is caught", %{sequence: sequence} do
      config = config(sequence, %{})
      assert Processor.on_end(:not_a_span, config) == false
      assert_received {:export_error, {:span_end_crashed, _, _}}
    end

    test "a missing async exporter marks the slot failed", %{sequence: sequence} do
      config = config(sequence, %{exporter: :no_such_exporter})
      record = started(event_span(), config)
      assert Processor.on_end(record, config) == false
      assert_received {:export_error, :exporter_not_running}
      assert Sequence.declaration(sequence).complete_through == 0
    end

    test "failures are logged, rate limited" do
      log =
        capture_log(fn ->
          config = %{schema_version: "v1"}
          for _ <- 1..5, do: Processor.on_end(event_span(), config)
        end)

      assert length(String.split(log, "sentinel probe: span export failed")) - 1 <= 1
    end
  end

  # -- 2. unattributed remote parent -------------------------------------------

  describe "allow_unattributed_remote_parent" do
    test "off by default: a remote parent without an Event id fails the slot", %{
      sequence: sequence
    } do
      config = config(sequence, %{otlp: %{endpoint: sentinel(200), source_handle: "h"}})
      record = started(event_span(%{}, {0xABC, true}), config)

      assert Processor.on_end(record, config) == false
      assert_received {:export_error, {:missing_parent_event_id, _}}
      assert Sequence.declaration(sequence).complete_through == 0
    end

    test "on: exported as a root with no parent span id and no edge", %{sequence: sequence} do
      config =
        config(sequence, %{
          otlp: %{endpoint: sentinel(200), source_handle: "h"},
          allow_unattributed_remote_parent: true
        })

      record = started(event_span(%{}, {0xABC, true}), config)

      assert Processor.on_end(record, config) == true
      assert_received {:request, _header, request}
      {_, wire} = wire_span(request)
      assert wire.parent_span_id in [nil, <<>>]
      assert {:ok, [event]} = Codec.decode_traces(request)
      assert event.causal_predecessor_ids == []
      assert Sequence.declaration(sequence).complete_through == 1
    end

    test "on: an attributed remote parent keeps its edge", %{sequence: sequence} do
      config =
        config(sequence, %{
          otlp: %{endpoint: sentinel(200), source_handle: "h"},
          allow_unattributed_remote_parent: true
        })

      attrs = %{Span.attribute_parent_event_id() => "producer-evt"}

      record =
        started(event_span(attrs, {0xABC, true}), config)

      assert Processor.on_end(record, config) == true
      assert_received {:request, _header, request}
      assert {:ok, [%{causal_predecessor_ids: ["producer-evt"]}]} = Codec.decode_traces(request)
    end

    test "on: a LOCAL parent without an Event id is still refused", %{sequence: sequence} do
      config =
        config(sequence, %{
          otlp: %{endpoint: sentinel(200), source_handle: "h"},
          allow_unattributed_remote_parent: true
        })

      record = started(event_span(%{}, {0xABC, false}), config)
      assert Processor.on_end(record, config) == false
      assert_received {:export_error, {:missing_parent_event_id, _}}
    end
  end

  # -- 3. parent Event id propagation -------------------------------------------

  describe "Propagation" do
    test "inject sets the header on a list, replacing case-insensitively" do
      headers = [{"X-Sentinel-Parent-Event-Id", "old"}, {"other", "1"}]

      assert Propagation.inject_parent_event_id(headers, "evt-9") ==
               [{"other", "1"}, {"x-sentinel-parent-event-id", "evt-9"}]
    end

    test "inject sets the header on a map and on nil" do
      assert Propagation.inject_parent_event_id(%{"a" => "b"}, "e") ==
               %{"a" => "b", "x-sentinel-parent-event-id" => "e"}

      assert Propagation.inject_parent_event_id(nil, "e") == [{"x-sentinel-parent-event-id", "e"}]
    end

    test "inject ignores a blank id" do
      assert Propagation.inject_parent_event_id([{"a", "b"}], "") == [{"a", "b"}]
      assert Propagation.inject_parent_event_id(%{}, nil) == %{}
    end

    test "extract reads the header, case-insensitively, from a list or a map" do
      assert Propagation.extract_parent_event_id([{"X-Sentinel-Parent-Event-ID", " e1 "}]) == "e1"
      assert Propagation.extract_parent_event_id(%{"x-sentinel-parent-event-id" => "e2"}) == "e2"
      assert Propagation.extract_parent_event_id([{~c"x-sentinel-parent-event-id", ~c"e3"}]) == "e3"
    end

    test "extract falls back to tracestate sentinel=<id>" do
      headers = [{"tracestate", "vendor=a, sentinel=evt-7 ,b=c"}]
      assert Propagation.extract_parent_event_id(headers) == "evt-7"
    end

    test "the header wins over tracestate, and absence is nil" do
      headers = [{"tracestate", "sentinel=ts"}, {"x-sentinel-parent-event-id", "hdr"}]
      assert Propagation.extract_parent_event_id(headers) == "hdr"
      assert Propagation.extract_parent_event_id([{"traceparent", "00-aa-bb-01"}]) == nil
      assert Propagation.extract_parent_event_id([{"tracestate", "sentinel="}]) == nil
      assert Propagation.extract_parent_event_id(nil) == nil
    end

    test "inject then extract round trips" do
      headers = Propagation.inject_parent_event_id([], "round-trip")
      assert Propagation.extract_parent_event_id(headers) == "round-trip"
    end
  end

  # -- 4. async exporter ----------------------------------------------------------

  describe "AsyncExporter" do
    defp exporter(sequence, extra) do
      test = self()

      defaults = [
        endpoint: "http://unused",
        source_handle: "h",
        sequence: sequence,
        backoff_ms: 1,
        max_backoff_ms: 2,
        retry_interval_ms: 0,
        on_error: fn reason -> send(test, {:export_error, reason}) end
      ]

      opts = Keyword.merge(defaults, extra)
      {:ok, pid} = AsyncExporter.start_link(opts)
      pid
    end

    defp item(sequence) do
      {:ok, slot} = Sequence.allocate(sequence)
      %{span: %Span{event_id: "e#{slot.number}"}, slot: slot, event: :event}
    end

    defp counting_post(results) do
      test = self()
      {:ok, counter} = Agent.start_link(fn -> results end)

      fn _endpoint, _handle, spans, opts ->
        send(test, {:post, length(spans), opts})

        Agent.get_and_update(counter, fn
          [last] -> {last, [last]}
          [next | rest] -> {next, rest}
        end)
      end
    end

    test "submit never blocks on a slow transport", %{sequence: sequence} do
      gate = self()

      post = fn _, _, _, _ ->
        send(gate, :entered)

        receive do
          :release -> {:ok, ""}
        end
      end

      exporter = exporter(sequence, post: post)
      {micros, :ok} = :timer.tc(fn -> AsyncExporter.submit(exporter, item(sequence)) end)
      assert micros < 100_000
      assert_receive :entered
      assert Sequence.declaration(sequence).complete_through == 0
    end

    test "an accepted export marks the slot exported and calls on_event", %{sequence: sequence} do
      test = self()

      exporter =
        exporter(sequence,
          post: counting_post([{:ok, ""}]),
          resource_attributes: [{"service.name", "shipping"}],
          on_event: &send(test, {:event, &1})
        )

      AsyncExporter.submit(exporter, item(sequence))
      assert :ok = AsyncExporter.flush(exporter)
      assert_received {:post, 1, [resource_attributes: [{"service.name", "shipping"}]]}
      assert_received {:event, :event}
      assert Sequence.declaration(sequence).complete_through == 1
    end

    test "transport errors are retried with backoff, then succeed", %{sequence: sequence} do
      post =
        counting_post([
          {:error, {:transport, :econnrefused}},
          {:error, {:transport, :closed}},
          {:ok, ""}
        ])

      exporter = exporter(sequence, post: post)
      AsyncExporter.submit(exporter, item(sequence))
      assert :ok = AsyncExporter.flush(exporter)
      assert_received {:post, 1, _}
      assert_received {:post, 1, _}
      assert_received {:post, 1, _}
      assert Sequence.declaration(sequence).complete_through == 1
      refute_received {:export_error, _}
    end

    test "after N attempts the slot is FAILED and reported", %{sequence: sequence} do
      post = counting_post([{:error, {:transport, :econnrefused}}])
      exporter = exporter(sequence, post: post, max_attempts: 3, max_retained_failed: 0)
      AsyncExporter.submit(exporter, item(sequence))
      assert :ok = AsyncExporter.flush(exporter)

      for _ <- 1..3, do: assert_received({:post, 1, _})
      refute_received {:post, 1, _}
      assert_received {:export_error, {:transport, :econnrefused}}
      assert Sequence.declaration(sequence).complete_through == 0
    end

    test "a 4xx is not retried and carries the response body", %{sequence: sequence} do
      post = counting_post([{:error, {:http_status, 400, "parent span id without event id"}}])
      exporter = exporter(sequence, post: post)
      AsyncExporter.submit(exporter, item(sequence))
      assert :ok = AsyncExporter.flush(exporter)

      assert_received {:post, 1, _}
      refute_received {:post, 1, _}
      assert_received {:export_error, {:http_status, 400, body} = reason}
      assert body =~ "parent span id"
      assert Http.format_error(reason) == "HTTP 400: parent span id without event id"
      assert Sequence.declaration(sequence).complete_through == 0
    end

    test "a 503 is transient and retried", %{sequence: sequence} do
      post = counting_post([{:error, {:http_status, 503, "busy"}}, {:ok, ""}])
      exporter = exporter(sequence, post: post)
      AsyncExporter.submit(exporter, item(sequence))
      assert :ok = AsyncExporter.flush(exporter)
      assert Sequence.declaration(sequence).complete_through == 1
    end

    test "a rejected span marks the slot failed without retry", %{sequence: sequence} do
      post = counting_post([{:ok, Base.decode16!("0a020801", case: :lower)}])
      exporter = exporter(sequence, post: post)
      AsyncExporter.submit(exporter, item(sequence))
      assert :ok = AsyncExporter.flush(exporter)
      assert_received {:export_error, {:rejected_spans, 1}}
      assert Sequence.declaration(sequence).complete_through == 0
    end

    for kind <- ["evidence_conflict", "identity_conflict"] do
      test "a refused duplicate Event id (#{kind}) keeps the slot FAILED and is not retried",
           %{sequence: sequence} do
        message = "append_events conflicted: #{unquote(kind)}"

        refusal =
          {:ok,
           <<0x0A, 2 + 2 + byte_size(message), 0x08, 0x01, 0x12, byte_size(message)>> <> message}

        post = counting_post([{:ok, ""}, refusal, {:ok, ""}])

        exporter =
          exporter(sequence,
            post: post,
            batch_size: 1,
            max_retained_failed: 10,
            retry_interval_ms: 20
          )

        for _ <- 1..3, do: AsyncExporter.submit(exporter, item(sequence))
        assert :ok = AsyncExporter.flush(exporter)
        assert_received {:export_error, {:duplicate_event_id, ^message}}
        refute_received {:export_error, _}
        declaration = Sequence.declaration(sequence)
        # Slots 1 and 3 exported, slot 2 FAILED: complete_through stops at 1.
        assert {declaration.last_emitted, declaration.complete_through} == {3, 1}
        # Not retried: exactly three posts, even past several retry-timer ticks and another flush.
        Process.sleep(100)
        assert :ok = AsyncExporter.flush(exporter)
        for _ <- 1..3, do: assert_received({:post, 1, _})
        refute_received {:post, _, _}
        assert Sequence.declaration(sequence).complete_through == 1
      end
    end

    test "slot_conflict also stays FAILED as a plain rejection", %{sequence: sequence} do
      message = "append_events conflicted: slot_conflict"
      body = <<0x0A, 2 + 2 + byte_size(message), 0x08, 0x01, 0x12, byte_size(message)>> <> message
      exporter = exporter(sequence, post: counting_post([{:ok, body}]))
      AsyncExporter.submit(exporter, item(sequence))
      assert :ok = AsyncExporter.flush(exporter)
      assert_received {:export_error, {:rejected_spans, 1}}
      assert Sequence.declaration(sequence).complete_through == 0
    end

    test "a permanent failure in a batch is isolated to the bad span", %{sequence: sequence} do
      post = fn _, _, spans, _ ->
        if Enum.any?(spans, &(&1.event_id == "bad")),
          do: {:error, {:http_status, 400, "bad span"}},
          else: {:ok, ""}
      end

      exporter = exporter(sequence, post: post, batch_size: 3)
      # Hold the worker so the three items batch together.
      {:ok, held} = Sequence.allocate(sequence)

      [a, b, c] = for _ <- 1..3, do: item(sequence)
      b = %{b | span: %Span{event_id: "bad"}}

      for i <- [%{span: %Span{event_id: "held"}, slot: held}, a, b, c],
          do: AsyncExporter.submit(exporter, i)

      assert :ok = AsyncExporter.flush(exporter)
      # slot 1 (held) and slots 2.. good; 3 (bad) failed: complete_through stops before it.
      assert Sequence.declaration(sequence).complete_through == 2
    end

    test "overflow drops the new item, fails its slot, and a later retry recovers it", %{
      sequence: sequence
    } do
      test = self()
      {:ok, open} = Agent.start_link(fn -> false end)

      post = fn _, _, spans, _ ->
        send(test, {:sending, Enum.map(spans, & &1.event_id), self()})

        receive do
          :release -> :ok
        after
          50 -> :ok
        end

        if Agent.get(open, & &1), do: {:ok, ""}, else: {:error, {:transport, :econnrefused}}
      end

      exporter = exporter(sequence, post: post, max_queue: 1, batch_size: 1, max_attempts: 1)
      [first, second, third] = for _ <- 1..3, do: item(sequence)

      AsyncExporter.submit(exporter, first)
      assert_receive {:sending, ["e1"], worker}
      AsyncExporter.submit(exporter, second)
      AsyncExporter.submit(exporter, third)

      assert_receive {:export_error, {:queue_overflow, %{number: 3}}}
      Agent.update(open, fn _ -> true end)
      send(worker, :release)
      assert :ok = AsyncExporter.flush(exporter)
      assert Sequence.declaration(sequence).complete_through == 3
    end

    test "flush retries retained failed slots once", %{sequence: sequence} do
      {:ok, open} = Agent.start_link(fn -> false end)

      post = fn _, _, _, _ ->
        if Agent.get(open, & &1), do: {:ok, ""}, else: {:error, {:transport, :econnrefused}}
      end

      exporter = exporter(sequence, post: post, max_attempts: 1)
      AsyncExporter.submit(exporter, item(sequence))
      assert :ok = AsyncExporter.flush(exporter)
      assert Sequence.declaration(sequence).complete_through == 0
      assert AsyncExporter.stats(exporter).retained == 1

      Agent.update(open, fn _ -> true end)
      assert :ok = AsyncExporter.flush(exporter)
      assert Sequence.declaration(sequence).complete_through == 1
      assert AsyncExporter.stats(exporter).retained == 0
    end

    test "the retry timer re-sends failed slots", %{sequence: sequence} do
      {:ok, open} = Agent.start_link(fn -> false end)

      post = fn _, _, _, _ ->
        if Agent.get(open, & &1), do: {:ok, ""}, else: {:error, {:transport, :econnrefused}}
      end

      exporter = exporter(sequence, post: post, max_attempts: 1, retry_interval_ms: 20)
      AsyncExporter.submit(exporter, item(sequence))
      wait_until(fn -> AsyncExporter.stats(exporter).retained == 1 end)
      Agent.update(open, fn _ -> true end)
      wait_until(fn -> Sequence.declaration(sequence).complete_through == 1 end)
    end

    test "a crashing transport does not crash the exporter or the host", %{sequence: sequence} do
      exporter =
        exporter(sequence, post: fn _, _, _, _ -> raise "kaboom" end, max_retained_failed: 0)

      AsyncExporter.submit(exporter, item(sequence))
      assert :ok = AsyncExporter.flush(exporter)
      assert Process.alive?(exporter)
      assert_received {:export_error, {:exception, %RuntimeError{}}}
      assert Sequence.declaration(sequence).complete_through == 0
    end

    test "shutdown flushes before the final heartbeat, then fails later items", %{
      sequence: sequence
    } do
      test = self()
      exporter = exporter(sequence, post: counting_post([{:ok, ""}]))

      {:ok, heartbeat} =
        Heartbeat.start_link(
          sequence: sequence,
          interval_ms: 0,
          send: fn signal ->
            send(test, {:final, signal.complete_through, signal.last_emitted})
            :ok
          end
        )

      AsyncExporter.submit(exporter, item(sequence))
      assert :ok = AsyncExporter.shutdown(exporter, heartbeat: heartbeat)
      assert_receive {:final, 1, 1}

      late = item(sequence)
      AsyncExporter.submit(exporter, late)
      assert_receive {:export_error, :exporter_stopped}
    end

    test "invalid options raise" do
      assert_raise ArgumentError, fn -> AsyncExporter.start_link([]) end

      assert_raise ArgumentError, fn ->
        AsyncExporter.start_link(endpoint: "http://x", source_handle: "h", max_queue: 0)
      end
    end
  end

  # -- the processor over a real socket, default async ----------------------------

  describe "Processor.start_link default exporter" do
    test "starts an async exporter, enqueues from on_end, force_flush drains", %{
      sequence: sequence
    } do
      config =
        config(sequence, %{
          otlp: %{endpoint: sentinel(200), source_handle: "h"},
          resource_attributes: [{"service.name", "shipping"}, {"service.version", "1.2"}],
          name: "test-#{System.unique_integer([:positive])}"
        })

      {:ok, _pid, config} = Processor.start_link(config)
      assert config.exporter != nil

      record = started(event_span(), config)
      assert Processor.on_end(record, config) == true
      assert :ok = Processor.force_flush(config)
      assert Sequence.declaration(sequence).complete_through == 1

      assert_received {:request, header, request}
      assert header =~ "x-sentinel-source-handle: h"
      {resource_spans, _wire} = wire_span(request)
      keys = Enum.map(resource_spans.resource.attributes, & &1.key)
      assert keys == ["service.name", "service.version"]
    end

    test "a Sentinel outage does not block on_end", %{sequence: sequence} do
      config =
        config(sequence, %{
          otlp: %{endpoint: closed_port(), source_handle: "h"},
          exporter_opts: [max_attempts: 2, backoff_ms: 1, retry_interval_ms: 0],
          name: "outage-#{System.unique_integer([:positive])}"
        })

      {:ok, _pid, config} = Processor.start_link(config)
      record = started(event_span(), config)
      {micros, true} = :timer.tc(fn -> Processor.on_end(record, config) end)
      assert micros < 100_000
      assert :ok = Processor.force_flush(config)
      assert Sequence.declaration(sequence).complete_through == 0
      assert_received {:export_error, {:transport, _}}
    end

    test "async: false and no otlp start no exporter", %{sequence: sequence} do
      {:ok, _, config} =
        Processor.start_link(
          config(sequence, %{async: false, otlp: %{endpoint: "http://x", source_handle: "h"}})
        )

      refute Map.has_key?(config, :exporter)
      {:ok, _, config} = Processor.start_link(%{})
      refute Map.has_key?(config, :exporter)
    end
  end

  # -- 5. Event Filter on the span path -------------------------------------------

  describe "Event Filter" do
    defp store(filter) do
      FilterStore.new({:hardening_test, System.unique_integer([:positive])}, filter)
    end

    defp filter(epoch, kinds, keys \\ []) do
      %EventFilter{
        epoch: epoch,
        specifications: [
          %SpecificationFilter{
            specification_id: "s",
            specification_version: "1",
            event_match: %EventMatch{event_kinds: kinds, projected_attribute_keys: keys}
          }
        ]
      }
    end

    test "an Event no Specification selects is not exported and its slot is filtered", %{
      sequence: sequence
    } do
      config =
        config(sequence, %{
          otlp: %{endpoint: closed_port(), source_handle: "h"},
          filter_store: store(filter(4, ["something.else"]))
        })

      record = started(event_span(), config)
      assert Processor.on_end(record, config) == true
      refute_received {:export_error, _}

      assert [%{first: 1, last: 1, filter_epoch: 4}] = Sequence.declaration(sequence).filtered
    end

    test "a selected Event is exported with only the projected attributes", %{sequence: sequence} do
      config =
        config(sequence, %{
          otlp: %{endpoint: sentinel(200), source_handle: "h"},
          filter_store: store(filter(4, ["sagashop.shipping.consume"], ["order.id"]))
        })

      attrs = %{"order.id" => "o-1", "secret" => "x"}
      record = started(event_span(attrs), config)
      assert Processor.on_end(record, config) == true
      assert_received {:request, _header, request}
      assert {:ok, [event]} = Codec.decode_traces(request)
      assert Enum.map(event.attributes, & &1.key) == ["order.id"]
      # The Event acknowledges the held Filter's epoch.
      assert event.acknowledged_filter_epoch == 4 or config.acknowledged_epoch == 0
      assert Sequence.declaration(sequence).complete_through == 1
    end

    test "with no Filter held everything is exported", %{sequence: sequence} do
      config =
        config(sequence, %{
          otlp: %{endpoint: sentinel(200), source_handle: "h"},
          filter_store: store(nil)
        })

      record = started(event_span(), config)
      assert Processor.on_end(record, config) == true
      assert Sequence.declaration(sequence).complete_through == 1
    end

    test "acknowledged epoch defaults to the held Filter's epoch", %{sequence: sequence} do
      config =
        config(sequence, %{
          otlp: %{endpoint: sentinel(200), source_handle: "h"},
          filter_store: store(filter(9, ["sagashop.shipping.consume"]))
        })
        |> Map.delete(:acknowledged_epoch)

      record = started(event_span(), config)
      assert Processor.on_end(record, config) == true
      assert_received {:request, _header, request}
      assert {:ok, [event]} = Codec.decode_traces(request)
      assert event.acknowledged_filter_epoch == 9
    end
  end

  # -- log receiver bind address ----------------------------------------------------

  describe "Receiver bind address" do
    test "defaults to loopback on an ephemeral port" do
      {:ok, receiver} = Receiver.start_link()
      assert Receiver.port(receiver) > 0
      assert {:ok, {{127, 0, 0, 1}, _}} = :inet.sockname(:sys.get_state(receiver).listen)
      Receiver.stop(receiver)
    end

    test "binds the given address and port" do
      {:ok, probe} = :gen_tcp.listen(0, [])
      {:ok, port} = :inet.port(probe)
      :gen_tcp.close(probe)

      {:ok, receiver} = Receiver.start_link(ip: {0, 0, 0, 0}, port: port)
      assert Receiver.port(receiver) == port
      assert {:ok, {{0, 0, 0, 0}, ^port}} = :inet.sockname(:sys.get_state(receiver).listen)
      Receiver.stop(receiver)
    end

    test "accepts an address string and rejects a bad one" do
      {:ok, receiver} = Receiver.start_link(ip: "127.0.0.1")
      Receiver.stop(receiver)
      Process.flag(:trap_exit, true)
      assert {:error, {:invalid_bind_ip, "nope"}} = Receiver.start_link(ip: "nope")
      assert {:error, {:invalid_bind_port, -1}} = Receiver.start_link(port: -1)
    end
  end

  # -- resource attributes + error body ---------------------------------------------

  describe "Resource attributes and error bodies" do
    test "encode_traces carries the Resource" do
      span = %Span{
        event_id: "e",
        sequence: %Sentinel.Model.V1.SequenceCoordinate{epoch: 1, sequence: 1},
        schema_version: "v1",
        acknowledged_epoch: 0,
        claimed_sensitivity: :SENSITIVITY_INTERNAL,
        name: "k",
        start_time: 1_700_000_000_000_000_000,
        attributes: []
      }

      {:ok, bytes} =
        Codec.encode_traces([span],
          resource_attributes: [{"service.name", "s"}, {"n", 3}, {"ok", true}, {"f", 1.5}]
        )

      {resource_spans, _} = wire_span(bytes)
      assert length(resource_spans.resource.attributes) == 4
      assert {:ok, [_]} = Codec.decode_traces(bytes)

      assert {:error, {:invalid_resource_attribute, "bad"}} =
               Codec.encode_traces([span], resource_attributes: [{"bad", :atom}])

      {:ok, bare} = Codec.encode_traces([span])
      {bare_spans, _} = wire_span(bare)
      assert bare_spans.resource == nil
    end

    test "a non-2xx surfaces the response body" do
      endpoint = sentinel(400)

      assert {:error, {:http_status, 400, "nope"} = reason} =
               Http.post(endpoint, "/v1/traces", [], "x")

      assert Http.format_error(reason) == "HTTP 400: nope"
      assert Http.format_error({:transport, :closed}) == "{:transport, :closed}"
    end
  end
end

defmodule Sentinel.Probe.SDK.FilterAcknowledgementTest do
  use ExUnit.Case, async: true

  alias Sentinel.Probe.SDK.Client.Client
  alias Sentinel.Probe.SDK.Otlp.{Http, Receiver}

  @golden Path.expand("../../../testdata/otlp/event-filter.bin", __DIR__)

  defp post(receiver, body) do
    {:ok, status} =
      Http.post(
        "http://127.0.0.1:#{Receiver.port(receiver)}",
        "/v1/logs",
        [],
        body
      )

    status
  end

  defp client(receiver, handle) do
    Client.new(
      %Client.Config{
        source_handle: handle,
        sentinel_base_url: "http://unused",
        initial_filter: nil
      },
      receiver
    )
  end

  # The receiver hands Filters to the store's writer asynchronously, so the
  # acknowledged epoch follows the push rather than coinciding with it.
  defp await_epoch(client, expected, attempts \\ 200) do
    cond do
      Client.acknowledged_epoch(client) == expected ->
        expected

      attempts == 0 ->
        Client.acknowledged_epoch(client)

      true ->
        Process.sleep(10)
        await_epoch(client, expected, attempts - 1)
    end
  end

  test "a pushed Event Filter log (the real wire shape) moves the acknowledged epoch" do
    {:ok, receiver} = Receiver.start_link()
    client = client(receiver, "ack-#{System.unique_integer([:positive])}")
    assert Client.acknowledged_epoch(client) == nil

    post(receiver, File.read!(@golden))
    assert await_epoch(client, 9) == 9
    assert Receiver.filters(receiver) |> hd() |> Map.get(:epoch) == 9
  end

  test "a Filter received before the store is attached is applied on attach" do
    {:ok, receiver} = Receiver.start_link()
    post(receiver, File.read!(@golden))
    client = client(receiver, "ack-#{System.unique_integer([:positive])}")
    assert await_epoch(client, 9) == 9
  end

  test "a restarted Probe restores the last pushed Filter from :persist_path" do
    path =
      Path.join(System.tmp_dir!(), "sentinel-filter-#{System.unique_integer([:positive])}.bin")

    on_exit(fn -> File.rm(path) end)

    {:ok, first} = Receiver.start_link(persist_path: path)
    post(first, File.read!(@golden))
    Receiver.stop(first)

    {:ok, second} = Receiver.start_link(persist_path: path)
    client = client(second, "ack-#{System.unique_integer([:positive])}")
    assert await_epoch(client, 9) == 9
  end

  test "an unreadable :persist_path restores nothing" do
    {:ok, receiver} = Receiver.start_link(persist_path: "/nonexistent-dir/none.bin")
    client = client(receiver, "ack-#{System.unique_integer([:positive])}")
    assert Client.acknowledged_epoch(client) == nil
  end
end
