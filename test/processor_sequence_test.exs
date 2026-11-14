defmodule Sentinel.Probe.SDK.ProcessorSequenceTest do
  use ExUnit.Case, async: true

  require Sentinel.Probe.SDK.Emission.Processor

  alias Sentinel.Model.V1.SequenceCoordinate
  alias Sentinel.Probe.SDK.Emission.{Processor, Sequence, Span}
  alias Sentinel.Probe.SDK.Otlp.{Codec, Http}

  @start_ms 1_794_000_000_000

  setup do
    {:ok, sequence} = Sequence.start_link(now: fn -> @start_ms * 1_000_000 end)
    %{sequence: sequence}
  end

  # A one-shot Sentinel on 127.0.0.1 that answers with `status` and `body` and
  # hands the request body to the test.
  defp sentinel(status, body) do
    test = self()
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(listen)

    spawn_link(fn ->
      {:ok, socket} = :gen_tcp.accept(listen)
      {:ok, _header, request} = Http.read_message(socket)
      send(test, {:request, request})

      :ok =
        :gen_tcp.send(
          socket,
          "HTTP/1.1 #{status} X\r\nContent-Length: #{byte_size(body)}\r\n\r\n" <> body
        )

      :gen_tcp.close(socket)
      :gen_tcp.close(listen)
    end)

    "http://127.0.0.1:#{port}"
  end

  defp config(sequence, origin, extra \\ %{}) do
    test = self()

    Map.merge(
      %{
        allocator: sequence,
        schema_version: "sagashop-contract-v1",
        acknowledged_epoch: 0,
        claimed_sensitivity: :SENSITIVITY_INTERNAL,
        otlp: %{endpoint: origin, source_handle: "sagashop-payment"},
        on_export_error: fn reason -> send(test, {:export_error, reason}) end
      },
      extra
    )
  end

  defp record(attributes) do
    Processor.span(
      name: "sagashop.payment.charge.completed",
      start_time: :opentelemetry.timestamp(),
      attributes: :otel_attributes.new(attributes, 128, :infinity),
      links: :otel_links.new([], 128, 128, :infinity)
    )
  end

  defp charge(extra \\ %{}),
    do: record(Map.merge(%{Span.attribute_event_id() => "01f0c0ffee-charge"}, extra))

  defp stamped(span_record) do
    attributes = :otel_attributes.map(Processor.span(span_record, :attributes))

    {Map.get(attributes, Span.attribute_sequence_epoch()),
     Map.get(attributes, Span.attribute_sequence_number())}
  end

  # A FAILED slot is not in flight, so marking it filtered is a no-op.
  defp assert_failed(sequence, slot) do
    assert Sequence.declaration(sequence).complete_through == 0
    :ok = Sequence.mark_filtered(sequence, slot, 1)
    assert Sequence.declaration(sequence).filtered == []
  end

  test "on_start stamps the slot and an accepted export marks it exported", %{sequence: sequence} do
    config = config(sequence, sentinel(200, ""))
    started = Processor.on_start(:otel_ctx.new(), charge(), config)
    assert stamped(started) == {@start_ms, 1}
    assert Sequence.declaration(sequence).complete_through == 0
    assert Processor.on_end(started, config) == true
    assert Sequence.declaration(sequence).complete_through == 1
    assert_received {:request, request}
    assert {:ok, [event]} = Codec.decode_traces(request)
    assert event.sequence == %SequenceCoordinate{epoch: @start_ms, sequence: 1}
  end

  test "a 200 with a rejected span marks the slot failed", %{sequence: sequence} do
    config = config(sequence, sentinel(200, Base.decode16!("0a020801", case: :lower)))
    started = Processor.on_start(:otel_ctx.new(), charge(), config)
    assert Processor.on_end(started, config) == false
    assert_received {:export_error, {:rejected_spans, 1}}
    assert_failed(sequence, %{epoch: @start_ms, number: 1})
  end

  test "an undecodable 200 marks the slot failed", %{sequence: sequence} do
    config = config(sequence, sentinel(200, Base.decode16!("0a0508", case: :lower)))
    started = Processor.on_start(:otel_ctx.new(), charge(), config)
    assert Processor.on_end(started, config) == false
    assert_received {:export_error, {:undecodable_response, _}}
    assert_failed(sequence, %{epoch: @start_ms, number: 1})
  end

  test "an HTTP failure marks the slot failed", %{sequence: sequence} do
    config = config(sequence, sentinel(500, "unavailable"))
    started = Processor.on_start(:otel_ctx.new(), charge(), config)
    assert Processor.on_end(started, config) == false
    assert_received {:export_error, {:http_status, 500, "unavailable"}}
    assert_failed(sequence, %{epoch: @start_ms, number: 1})
  end

  test "a span that fails conversion marks the slot failed and posts nothing", %{
    sequence: sequence
  } do
    config = config(sequence, "http://127.0.0.1:9")
    started = Processor.on_start(:otel_ctx.new(), record(%{}), config)
    assert Processor.on_end(started, config) == false
    assert_received {:export_error, _}
    assert_failed(sequence, %{epoch: @start_ms, number: 1})
  end

  test "a host-stamped span keeps its slot", %{sequence: sequence} do
    {:ok, reserved} = Sequence.allocate(sequence)

    host =
      charge(%{
        Span.attribute_sequence_epoch() => reserved.epoch,
        Span.attribute_sequence_number() => reserved.number
      })

    config = config(sequence, sentinel(200, ""))
    started = Processor.on_start(:otel_ctx.new(), host, config)
    assert Sequence.highest_allocated(sequence) == 1
    assert stamped(started) == {reserved.epoch, reserved.number}
    assert Processor.on_end(started, config) == true
    assert Sequence.declaration(sequence).complete_through == 1
  end

  test "a configured :sequence is not overridden by the allocator", %{sequence: sequence} do
    coordinate = %SequenceCoordinate{epoch: 7, sequence: 1}
    config = config(sequence, sentinel(200, ""), %{sequence: coordinate})
    started = Processor.on_start(:otel_ctx.new(), charge(), config)
    assert stamped(started) == {nil, nil}
    assert Sequence.highest_allocated(sequence) == 0
    assert Processor.on_end(started, config) == true
    assert_received {:request, request}
    assert {:ok, [%{sequence: ^coordinate}]} = Codec.decode_traces(request)
  end

  test "without an allocator on_start leaves the span alone" do
    span_record = charge()
    assert Processor.on_start(:otel_ctx.new(), span_record, %{}) == span_record
  end
end
