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
  adapter. The conversion tests construct `%Span{}` directly, exactly as the Go
  analog's `SpanToEvent` is tested via `tracetest.SpanStub` rather than a live
  TracerProvider; the Sequence allocation tests call `on_start/3` and `on_end/2`
  on records built with this module's own `span/1` macro.

  ## Config

  A map. Each host field may be a value or a zero-arg function (resolved at
  `on_end` so per-span dynamic values like `acknowledged_epoch` stay live):

    * `:schema_version`, `:claimed_capabilities`, `:claimed_sensitivity`
    * `:sequence` — `nil` or a function returning a `SequenceCoordinate`
    * `:acknowledged_epoch` — `nil`, an integer, or a function
    * `:on_malformed_link` — `(source, span_id) -> term()` or `nil`
    * `:on_event` — `(ProducerEvent.t() -> term())` or `nil`
    * `:allocator` — `nil` or a `Sentinel.Probe.SDK.Emission.Sequence` (#461)
    * `:otlp` — `%{endpoint: origin, source_handle: handle}`
    * `:resource_attributes` — `[{key, value}]` OTLP Resource attributes on every export
    * `:filter_store` — a `Sentinel.Probe.SDK.Client.FilterStore`; see "Event Filter"
    * `:allow_unattributed_remote_parent` — see "Unattributed remote parent"
    * `:exporter` — the name or pid of a `Sentinel.Probe.SDK.Otlp.AsyncExporter`; see "Export"
    * `:async` — `false` keeps the exporter out of `start_link/1` (default `true`)
    * `:exporter_opts` — extra `AsyncExporter` options (`:max_queue`, `:max_attempts`, ...)
    * `:on_export_error` — `(reason -> term())` for every export failure

  ## Export

  The OTel provider starts this processor through `start_link/1` (it is
  optional in the processor behaviour) when `:otlp` is configured. That starts
  an `AsyncExporter` (a bounded queue, a worker that does the HTTP, retries
  with backoff) under the provider's supervisor and puts its name in the
  config as `:exporter`. `on_end/2` then only enqueues. A host that wants the
  exporter in its own supervision tree starts one itself and passes it as
  `:exporter`, or sets `async: false` for the older synchronous export (one
  POST per span inside `on_end/2`, with a 5 s timeout) which is only suitable
  for tests.

  ## Never raise from the span end path

  `on_start/3` and `on_end/2` catch every error and exit, including a dead
  allocator or exporter and a raising callback. The slot is marked failed (so
  `complete_through` never passes it), the error goes to `:on_export_error` and
  to a rate-limited log, and nothing propagates into the host's process.

  ## Unattributed remote parent

  A span whose REMOTE parent is not a Probe carries a trace context but no
  `sentinel.parent.event.id`. By default `on_end/2` refuses it (the slot fails).
  With `allow_unattributed_remote_parent: true` it is exported as a causal
  root: the parent span id is dropped and no causal edge is recorded, because
  the periphery also refuses a parent span id without a parent Event id. A LOCAL
  parent without the id is always refused.

  ## Event Filter

  With `:filter_store` and a held Event Filter, `on_end/2` projects the Event
  with the Filter. An Event no Specification selects is not exported and its
  slot is marked filtered under the held Filter's epoch; a selected Event is
  exported with only the projected attributes. With no Filter held, everything
  is exported. When `:acknowledged_epoch` is not configured it is the held
  Filter's epoch.

  ## Sequence allocation

  With `:allocator` and no `:sequence`, `on_start/3` allocates a slot and sets
  `sentinel.sequence.epoch` and `sentinel.sequence.number` on the span record
  when the span starts, before any Event Filter, unless the host already set
  both (a slot reserved ahead of its Event). `on_end/2` exports that stamped
  slot. With `:allocator`, the slot `on_end/2` exported is marked exported only
  on HTTP 2xx with zero `rejected_spans`, and failed otherwise (a conversion
  error, an HTTP failure, a rejected span or an undecodable response, each
  reported through `:on_export_error`). Without `:otlp` the slot is left in
  flight for the host to mark. Without `:allocator`, nothing here changes.
  """

  @behaviour :otel_span_processor

  require Record

  # The OTel SDK's span and link records. Extracted at compile time from the
  # opentelemetry dep's headers so `on_end/2` can read fields by name. `from_lib`
  # resolves the header through the code path, so it also compiles when this SDK is
  # itself a dependency (a path or Hex dep of a host), where there is no `./deps`.
  Record.defrecord(
    :span,
    :span,
    Record.extract(:span, from_lib: "opentelemetry/include/otel_span.hrl")
  )

  Record.defrecord(
    :link,
    :link,
    Record.extract(:link, from_lib: "opentelemetry/include/otel_span.hrl")
  )

  alias Sentinel.Model.V1.SequenceCoordinate
  alias Sentinel.Model.V1.EventFilter
  alias Sentinel.Probe.SDK.Client.FilterStore
  alias Sentinel.Probe.SDK.Emission.{Sequence, Span, SpanToEvent}
  alias Sentinel.Probe.SDK.Filter
  alias Sentinel.Probe.SDK.Internal.ErrorLog
  alias Sentinel.Probe.SDK.Otlp.{AsyncExporter, Exporter, Http, PartialSuccess}

  @doc """
  Starts the span processor's exporter (an optional processor callback the OTel
  provider calls). Returns the config with `:exporter` set. Without `:otlp`, or
  with `async: false` or an `:exporter` already given, nothing is exported
  from here and an idle process stands in.
  """
  def start_link(config) when is_map(config) do
    case config do
      %{otlp: %{endpoint: _, source_handle: _} = otlp} when not is_map_key(config, :exporter) ->
        if Map.get(config, :async, true) == false,
          do: idle(config),
          else: start_exporter(config, otlp)

      _ ->
        idle(config)
    end
  end

  defp idle(config) do
    with {:ok, pid} <- Agent.start_link(fn -> :idle end), do: {:ok, pid, config}
  end

  defp start_exporter(config, otlp) do
    name = exporter_name(config)

    opts =
      [
        name: name,
        otlp: otlp,
        sequence: config[:allocator],
        resource_attributes: config[:resource_attributes] || [],
        on_error: config[:on_export_error],
        on_event: config[:on_event]
      ] ++ Keyword.new(config[:exporter_opts] || [])

    with {:ok, pid} <- AsyncExporter.start_link(opts) do
      {:ok, pid, Map.put(config, :exporter, name)}
    end
  end

  defp exporter_name(%{exporter_name: name}) when is_atom(name) and name != nil, do: name

  defp exporter_name(config) do
    suffix = config |> Map.get(:name) |> to_string() |> String.replace(~r/[^A-Za-z0-9_.<>-]/, "_")

    suffix =
      if suffix == "", do: Integer.to_string(System.unique_integer([:positive])), else: suffix

    String.to_atom("sentinel_probe_exporter_" <> suffix)
  end

  @doc false
  @impl true
  def on_start(_ctx, span_record, config) do
    allocator = config[:allocator]

    if allocator != nil and config[:sequence] == nil and stamped_slot(span_record) == nil do
      stamp(span_record, allocator)
    else
      span_record
    end
  catch
    kind, reason ->
      report(config, {:on_start_failed, kind, reason})
      span_record
  end

  # An allocation that fails (sequence-overflow) leaves the span unstamped, so
  # on_end refuses it as missing a sequence instead of crashing the host.
  defp stamp(span_record, allocator) do
    case Sequence.allocate(allocator) do
      {:ok, %{epoch: epoch, number: number}} ->
        attributes =
          span(span_record, :attributes)
          |> then(&:otel_attributes.set(Span.attribute_sequence_epoch(), epoch, &1))
          |> then(&:otel_attributes.set(Span.attribute_sequence_number(), number, &1))

        span(span_record, attributes: attributes)

      {:error, _} ->
        span_record
    end
  end

  defp stamped_slot(span_record) do
    attrs_map = attributes_map(span(span_record, :attributes))

    case {Map.get(attrs_map, Span.attribute_sequence_epoch()),
          Map.get(attrs_map, Span.attribute_sequence_number())} do
      {epoch, number} when is_integer(epoch) and is_integer(number) ->
        %SequenceCoordinate{epoch: epoch, sequence: number}

      _ ->
        nil
    end
  end

  @doc false
  @impl true
  def on_end(span_record, config) do
    handle_end(span_record, config)
  catch
    kind, reason ->
      # The slot may be unknown if the crash came before it was read; then it stays in flight,
      # which also holds complete_through back.
      fail_slot(config, slot_of(span_record, config))
      report(config, {:span_end_crashed, kind, reason})
      false
  end

  defp handle_end(span_record, config) do
    name = span(span_record, :name)
    start_time = to_start_time(span(span_record, :start_time))
    parent_span_id = to_span_id(span(span_record, :parent_span_id))

    attrs_map = attributes_map(span(span_record, :attributes))

    event_id = string_attr(attrs_map, Span.attribute_event_id())
    parent_event_id = string_attr(attrs_map, Span.attribute_parent_event_id())

    links = link_predecessors(span(span_record, :links))

    parent_span_id =
      if unattributed_remote_root?(config, span_record, parent_event_id),
        do: nil,
        else: parent_span_id

    span_struct = %Span{
      event_id: event_id,
      sequence: sequence(config, span_record),
      schema_version: resolve(config, :schema_version),
      acknowledged_epoch: acknowledged_epoch(config),
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

    with {:ok, event} <- SpanToEvent.span_to_event(span_struct),
         {:ok, span_struct, event} <- project(config, span_struct, event) do
      export(config, span_struct, event)
    else
      :filtered ->
        true

      {:error, reason} ->
        mark(config, span_struct, &Sequence.mark_failed/2)
        report(config, reason)
        false
    end
  end

  # A remote parent with no parent Event id is a system boundary: the caller is not a Probe.
  defp unattributed_remote_root?(config, span_record, parent_event_id) do
    config[:allow_unattributed_remote_parent] == true and parent_event_id == nil and
      span(span_record, :parent_span_is_remote) == true
  end

  defp acknowledged_epoch(config) do
    case {Map.has_key?(config, :acknowledged_epoch), config[:filter_store]} do
      {false, %FilterStore{} = store} -> FilterStore.epoch(store)
      _ -> resolve(config, :acknowledged_epoch)
    end
  end

  # Applies the held Event Filter. No store or no held Filter exports everything.
  defp project(%{filter_store: %FilterStore{} = store} = config, %Span{} = span_struct, event) do
    case FilterStore.get(store) do
      nil ->
        {:ok, span_struct, event}

      %EventFilter{} = filter ->
        case Filter.apply_filter(event, filter) do
          nil ->
            mark_filtered(config, span_struct, filter)
            :filtered

          projected ->
            kept = MapSet.new(projected.attributes, & &1.key)

            attributes =
              Enum.filter(span_struct.attributes || [], fn {key, _} ->
                Span.reserved_attribute?(key) or MapSet.member?(kept, key)
              end)

            {:ok, %Span{span_struct | attributes: attributes}, projected}
        end
    end
  end

  defp project(_config, span_struct, event), do: {:ok, span_struct, event}

  defp mark_filtered(config, span_struct, filter) do
    case {config[:allocator], span_struct.sequence} do
      {allocator, %SequenceCoordinate{} = coordinate} when allocator != nil ->
        slot = %{epoch: coordinate.epoch, number: coordinate.sequence}
        Sequence.mark_filtered(allocator, slot, filter.epoch || 0)

      _ ->
        :ok
    end
  end

  # With an allocator and no configured :sequence, the span exports the slot
  # on_start stamped.
  defp sequence(config, span_record) do
    configured = resolve(config, :sequence)

    if configured == nil and config[:allocator] != nil,
      do: stamped_slot(span_record),
      else: configured
  end

  defp mark(%{allocator: allocator}, %Span{sequence: %SequenceCoordinate{} = coordinate}, mark)
       when allocator != nil do
    mark.(allocator, %{epoch: coordinate.epoch, number: coordinate.sequence})
    :ok
  catch
    _, _ -> :ok
  end

  defp mark(_config, _span, _mark), do: :ok

  defp fail_slot(%{allocator: allocator}, %{epoch: _, number: _} = slot) when allocator != nil do
    Sequence.mark_failed(allocator, slot)
    :ok
  catch
    _, _ -> :ok
  end

  defp fail_slot(_config, _slot), do: :ok

  defp slot_of(span_record, config) do
    case sequence(config, span_record) do
      %SequenceCoordinate{epoch: epoch, sequence: number} -> %{epoch: epoch, number: number}
      _ -> nil
    end
  catch
    _, _ -> nil
  end

  # Every failure goes to the host's callback (which may itself raise) and a rate-limited log.
  defp report(config, reason) do
    if cb = config[:on_export_error] do
      try do
        cb.(reason)
      catch
        _, _ -> :ok
      end
    end

    ErrorLog.warn(:span_processor, "span export failed: " <> Http.format_error(reason))
  end

  # Async: only enqueue; the exporter marks the slot and reports failures.
  defp export(%{exporter: exporter} = config, span_struct, event) when exporter != nil do
    if exporter_alive?(exporter) do
      item = %{span: span_struct, event: event, slot: slot(span_struct)}
      AsyncExporter.submit(exporter, item)
      true
    else
      mark(config, span_struct, &Sequence.mark_failed/2)
      report(config, :exporter_not_running)
      false
    end
  end

  defp export(config, span_struct, event) do
    case export_otlp(config, span_struct) do
      :skip ->
        callback(config[:on_event], event)
        true

      :ok ->
        mark(config, span_struct, &Sequence.mark_exported/2)
        callback(config[:on_event], event)
        true

      {:error, reason} ->
        mark(config, span_struct, &Sequence.mark_failed/2)
        report(config, reason)
        false
    end
  end

  defp callback(nil, _arg), do: :ok

  defp callback(cb, arg) do
    cb.(arg)
    :ok
  catch
    _, _ -> :ok
  end

  defp slot(%Span{sequence: %SequenceCoordinate{} = c}), do: %{epoch: c.epoch, number: c.sequence}
  defp slot(_), do: nil

  defp exporter_alive?(pid) when is_pid(pid), do: Process.alive?(pid)
  defp exporter_alive?(name) when is_atom(name), do: Process.whereis(name) != nil
  defp exporter_alive?(_), do: true

  defp export_otlp(config, span) do
    case config[:otlp] do
      %{endpoint: endpoint, source_handle: source_handle} ->
        opts = [resource_attributes: config[:resource_attributes] || []]

        case Exporter.export_traces(endpoint, source_handle, [span], opts) do
          {:ok, body} -> accepted(config, body)
          {:error, reason} -> {:error, reason}
        end

      _ ->
        :skip
    end
  end

  # Without an allocator any 2xx counts, as before. With one, the export
  # succeeded only with zero rejected spans in a decodable response.
  defp accepted(%{allocator: allocator}, body) when allocator != nil do
    case PartialSuccess.rejected_spans(body) do
      {:ok, 0} -> :ok
      {:ok, rejected} -> {:error, {:rejected_spans, rejected}}
      {:error, reason} -> {:error, {:undecodable_response, reason}}
    end
  end

  defp accepted(_config, _body), do: :ok

  @doc false
  @impl true
  def force_flush(%{exporter: exporter}) when exporter != nil do
    case AsyncExporter.flush(exporter) do
      :ok -> :ok
      {:error, _} = error -> error
    end
  end

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
