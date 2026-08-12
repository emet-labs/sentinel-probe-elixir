defmodule Sentinel.Probe.SDK.Emission.Processor do
  @moduledoc """
  A custom `:otel_span_processor` whose `on_end/2` captures an ended span into a
  `Sentinel.Probe.SDK.Emission.Span` and converts it to a `ProducerEvent`.

  This is the BEAM-specific glue that lets the Emission package "wrap" the OTel
  Erlang SDK (ADR-0002: the host owns export). On BEAM you do not get a
  `ReadOnlySpan` object back the way the Go analog does; you register a span
  processor whose `on_end/2` receives the OTel `#span{}` record. This processor
  reads `name`, `start_time`, `attributes`, `parent_span_id` and `links` from
  that record, merges the host's per-call conversion fields from its config, and
  hands the resulting `ProducerEvent` to the host's `:on_event` callback.

  It is deliberately a STATELESS processor: it exports no `start_link/1`, so the
  OTel tracer provider treats it as a plain module (no process is started).
  `span_to_event/1` is the pure, hermetically-tested core; this is the runtime
  adapter and is not exercised by the test suite (which constructs `%Span{}`
  directly), exactly as the Go analog's `SpanToEvent` is tested via
  `tracetest.SpanStub` rather than a live TracerProvider.

  ## Config

  A map. Each host field may be a value or a zero-arg function (resolved at
  `on_end` so per-span dynamic values like `acknowledged_epoch` stay live):

    * `:schema_version`, `:claimed_capabilities`, `:claimed_sensitivity`
    * `:sequence` — `nil` or a function returning a `SequenceCoordinate`
    * `:acknowledged_epoch` — `nil`, an integer, or a function
    * `:on_malformed_link` — `(source, span_id) -> term()` or `nil`
    * `:on_event` — `(ProducerEvent.t() -> term())` or `nil`
  """

  @behaviour :otel_span_processor

  require Record

  # The OTel SDK's span and link records. Extracted at compile time from the
  # opentelemetry dep's headers so `on_end/2` can read fields by name.
  Record.defrecord(
    :span,
    :span,
    Record.extract(:span, from: "deps/opentelemetry/include/otel_span.hrl")
  )

  Record.defrecord(
    :link,
    :link,
    Record.extract(:link, from: "deps/opentelemetry/include/otel_span.hrl")
  )

  alias Sentinel.Probe.SDK.Emission.{Span, SpanToEvent}

  @doc false
  @impl true
  def on_start(_ctx, span, _config), do: span

  @doc false
  @impl true
  def on_end(span_record, config) do
    name = span(span_record, :name)
    start_time = to_start_time(span(span_record, :start_time))
    parent_span_id = to_span_id(span(span_record, :parent_span_id))

    attrs_map = attributes_map(span(span_record, :attributes))

    event_id = string_attr(attrs_map, Span.attribute_event_id())
    parent_event_id = string_attr(attrs_map, Span.attribute_parent_event_id())

    links = link_predecessors(span(span_record, :links))

    span_struct = %Span{
      event_id: event_id,
      sequence: resolve(config, :sequence),
      schema_version: resolve(config, :schema_version),
      acknowledged_epoch: resolve(config, :acknowledged_epoch),
      claimed_capabilities: resolve(config, :claimed_capabilities) || [],
      claimed_sensitivity: resolve(config, :claimed_sensitivity) || :SENSITIVITY_UNSPECIFIED,
      name: name,
      start_time: start_time,
      attributes: to_attributes(attrs_map),
      parent_event_id: parent_event_id,
      parent_span_id: parent_span_id,
      links: links,
      on_malformed_link: config[:on_malformed_link]
    }

    event = SpanToEvent.span_to_event(span_struct)

    if cb = config[:on_event], do: cb.(event)
    true
  end

  @doc false
  @impl true
  def force_flush(_config), do: :ok

  # A config field may be a value or a zero-arg function resolved at on_end.
  defp resolve(config, key) do
    case Map.fetch(config, key) do
      {:ok, fun} when is_function(fun, 0) -> fun.()
      {:ok, value} -> value
      :error -> nil
    end
  end

  defp to_start_time(nil), do: nil

  defp to_start_time(monotonic) do
    # Convert the OTel native monotonic start time to unix nanoseconds. This is
    # the exact POSIX-time conversion the OTel API documents for export.
    :opentelemetry.convert_timestamp(monotonic, :nanosecond)
  end

  # Render a 64-bit span id as the 16-char lowercase hex string Go's SpanID uses.
  defp to_span_id(nil), do: nil
  defp to_span_id(:undefined), do: nil

  defp to_span_id(span_id) when is_integer(span_id) do
    span_id |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(16, "0")
  end

  defp attributes_map(nil), do: %{}
  defp attributes_map(:undefined), do: %{}

  defp attributes_map(record) do
    # otel_attributes.map/1 returns the {key => value} map. Guard defensively.
    case :otel_attributes.map(record) do
      map when is_map(map) -> map
      _ -> %{}
    end
  end

  defp string_attr(attrs_map, key) do
    case Map.fetch(attrs_map, key) do
      {:ok, value} when is_binary(value) -> value
      _ -> nil
    end
  end

  defp to_attributes(attrs_map) do
    Enum.flat_map(attrs_map, fn {key, value} ->
      case to_tagged(value) do
        nil -> []
        tagged -> [{to_key(key), tagged}]
      end
    end)
  end

  defp to_key(key) when is_atom(key), do: Atom.to_string(key)
  defp to_key(key) when is_binary(key), do: key

  # OTel attribute values are untyped at the BEAM boundary; we recover the
  # AttributeValue oneof arm from the Elixir term. Elixir cannot distinguish a
  # string from bytes here (both binary), so binaries become :string — a
  # documented runtime limitation. The hermetic path constructs %Span{} with
  # explicit {:bytes, _} tags, which is exact.
  defp to_tagged(nil), do: nil
  defp to_tagged(v) when is_boolean(v), do: {:bool, v}
  defp to_tagged(v) when is_integer(v), do: {:int, v}
  defp to_tagged(v) when is_float(v), do: {:double, v}
  defp to_tagged(v) when is_binary(v), do: {:string, v}

  defp to_tagged(v) when is_list(v) do
    members = Enum.map(v, &to_tagged/1) |> Enum.reject(&(&1 == nil))
    {:array, members}
  end

  defp to_tagged(v) when is_map(v) do
    entries =
      Enum.flat_map(v, fn {k, val} ->
        case to_tagged(val) do
          nil -> []
          tagged -> [{to_key(k), tagged}]
        end
      end)

    {:map, entries}
  end

  defp to_tagged(v) when is_atom(v), do: {:string, Atom.to_string(v)}
  defp to_tagged(_), do: nil

  defp link_predecessors(nil), do: nil

  defp link_predecessors(links_record) do
    case :otel_links.list(links_record) do
      list when is_list(list) -> Enum.map(list, &link_predecessor/1)
      _ -> []
    end
  end

  defp link_predecessor(link_record) do
    span_id = to_span_id(link(link_record, :span_id))
    event_id = link_attributes_event_id(link(link_record, :attributes))
    {event_id, span_id}
  end

  defp link_attributes_event_id(nil), do: nil

  defp link_attributes_event_id(record) do
    case :otel_attributes.map(record) do
      map when is_map(map) -> string_attr(map, Span.attribute_event_id())
      _ -> nil
    end
  end
end
