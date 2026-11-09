defmodule Sentinel.Probe.SDK.Otlp.Codec do
  @moduledoc """
  Encodes ended spans and producer completeness as OTLP/HTTP protobuf, and
  decodes the log records Sentinel sends back.

  A missing predecessor id is an error. A span id is never stored as an Event
  id. Required span attributes are the #398 contract. Any other `sentinel.*`
  key rejects the export.
  """

  alias Sentinel.Model.V1.{
    EventFilter,
    OccurrenceTime,
    ProducerCompletenessSignal,
    ProducerEvent,
    SequenceCoordinate,
    Verdict
  }

  alias Sentinel.Probe.SDK.Emission.{Span, SpanToEvent}
  alias Sentinel.Probe.SDK.Int128
  alias Sentinel.Probe.SDK.Otlp.Wire
  alias Sentinel.Probe.V1.DecideResponse

  @payload_type "sentinel.payload.type"
  @request_id "sentinel.request_id"
  @source_handle "sentinel.source_handle"

  @completeness "sentinel.model.v1.ProducerCompletenessSignal"
  @event_filter "sentinel.model.v1.EventFilter"
  @verdict "sentinel.model.v1.Verdict"
  @decide "sentinel.probe.v1.DecideResponse"

  @sensitivities %{
    :SENSITIVITY_PUBLIC => "SENSITIVITY_PUBLIC",
    :SENSITIVITY_INTERNAL => "SENSITIVITY_INTERNAL",
    :SENSITIVITY_CONFIDENTIAL => "SENSITIVITY_CONFIDENTIAL",
    :SENSITIVITY_RESTRICTED => "SENSITIVITY_RESTRICTED"
  }

  @capabilities %{
    :SOURCE_CAPABILITY_OBSERVE_BEFORE_EFFECT => "SOURCE_CAPABILITY_OBSERVE_BEFORE_EFFECT",
    :SOURCE_CAPABILITY_CAUSAL_EDGES => "SOURCE_CAPABILITY_CAUSAL_EDGES",
    :SOURCE_CAPABILITY_COMPLETENESS_SIGNALS => "SOURCE_CAPABILITY_COMPLETENESS_SIGNALS",
    :SOURCE_CAPABILITY_BOUNDED_CLOCK_UNCERTAINTY => "SOURCE_CAPABILITY_BOUNDED_CLOCK_UNCERTAINTY"
  }

  @int64_min -9_223_372_036_854_775_808
  @int64_max 9_223_372_036_854_775_807
  @u64_max 18_446_744_073_709_551_615
  @end_delta 5_000_000

  @doc "Payload type for a producer completeness signal."
  @spec completeness_type :: String.t()
  def completeness_type, do: @completeness

  @doc "Payload type for an Event Filter."
  @spec event_filter_type :: String.t()
  def event_filter_type, do: @event_filter

  @doc "Payload type for a Verdict."
  @spec verdict_type :: String.t()
  def verdict_type, do: @verdict

  @doc "Payload type for a DecideResponse."
  @spec decide_type :: String.t()
  def decide_type, do: @decide

  @doc """
  Encodes ended spans as one uncompressed `ExportTraceServiceRequest`.

  The whole request fails when any span fails. Nothing in the bytes uses a
  span id as `sentinel.event.id` or as a causal predecessor.
  """
  @spec encode_traces([Span.t()]) :: {:ok, binary()} | {:error, term()}
  def encode_traces(spans) when is_list(spans) do
    with {:ok, encoded} <- encode_each(spans, []) do
      request = %Wire.ExportTraceServiceRequest{
        resource_spans: [
          %Wire.ResourceSpans{
            scope_spans: [%Wire.ScopeSpans{spans: Enum.reverse(encoded)}]
          }
        ]
      }

      {:ok, iodata(request)}
    end
  end

  @doc """
  Decodes an uncompressed trace export into `ProducerEvent`s.

  Gzip and JSON are errors. A span id is not accepted as causality.
  """
  @spec decode_traces(binary()) :: {:ok, [ProducerEvent.t()]} | {:error, term()}
  def decode_traces(bytes) when is_binary(bytes) do
    with :ok <- reject_wrapped(bytes),
         {:ok, request} <- decode_message(bytes, Wire.ExportTraceServiceRequest) do
      decode_spans(list(request.resource_spans), 0, [])
    end
  end

  @doc """
  Builds an active producer completeness signal.

  `start_ns` is the producer clock, pinned by the caller. This function does
  not read a clock. `last` is both last emitted and complete through.
  """
  @spec completeness_signal(non_neg_integer(), non_neg_integer(), non_neg_integer(), integer()) ::
          ProducerCompletenessSignal.t()
  def completeness_signal(sequence_epoch, last, filter_epoch, start_ns)
      when is_integer(sequence_epoch) and is_integer(last) and is_integer(filter_epoch) and
             is_integer(start_ns) do
    reported = SpanToEvent.build_occurrence_time(start_ns)

    %ProducerCompletenessSignal{
      sequence_epoch: sequence_epoch,
      last_emitted: last,
      complete_through: last,
      producer_reported_at: reported,
      acknowledged_filter_epoch: filter_epoch,
      activity: :SOURCE_ACTIVITY_STATE_ACTIVE,
      activity_since: reported
    }
  end

  @doc """
  Encodes a completeness signal as an OTLP log export.

  `sentinel.source_handle` is the deployment handle and must equal the
  `x-sentinel-source-handle` header the caller sends.
  """
  @spec encode_completeness(ProducerCompletenessSignal.t(), String.t()) ::
          {:ok, binary()} | {:error, term()}
  def encode_completeness(%ProducerCompletenessSignal{} = signal, source_handle)
      when is_binary(source_handle) do
    encode_log(Protobuf.encode(signal), @completeness,
      source_handle: source_handle,
      request_id: nil
    )
  end

  @doc """
  Encodes a `DecideResponse` as an OTLP log.

  Unspecified aggregate or specification action is an error. `sentinel.request_id`
  equals the message `request_id`.
  """
  @spec encode_decide(DecideResponse.t()) :: {:ok, binary()} | {:error, term()}
  def encode_decide(%DecideResponse{} = response) do
    with :ok <- require_specified(response) do
      encode_log(Protobuf.encode(response), @decide,
        source_handle: nil,
        request_id: response.request_id
      )
    end
  end

  @doc """
  Decodes an uncompressed log export into probe payloads.

  Accepted records are `{:verdict, Verdict.t()}`, `{:event_filter, EventFilter.t()}`,
  and `{:decide, DecideResponse.t()}`. Completeness is not a payload this
  function returns to the Probe: that signal is Probe → Sentinel. An empty
  export is `{:ok, []}`, which is zero records and not a pass.
  """
  @spec decode_probe_logs(binary()) :: {:ok, [term()]} | {:error, term()}
  def decode_probe_logs(bytes) when is_binary(bytes) do
    with :ok <- reject_wrapped(bytes),
         {:ok, records} <- decode_log_records(bytes) do
      decode_probe_records(records, [])
    end
  end

  @doc "Decodes log records without interpreting the body, for golden checks."
  @spec decode_log_records(binary()) :: {:ok, [map()]} | {:error, term()}
  def decode_log_records(bytes) when is_binary(bytes) do
    with :ok <- reject_wrapped(bytes),
         {:ok, request} <- decode_message(bytes, Wire.ExportLogsServiceRequest) do
      read_records(list(request.resource_logs), [])
    end
  end

  defp encode_each([], acc), do: {:ok, acc}

  defp encode_each([span | rest], acc) do
    case encode_span(span) do
      {:ok, encoded} -> encode_each(rest, [encoded | acc])
      {:error, reason} -> {:error, reason}
    end
  end

  defp encode_span(%Span{} = span) do
    with {:ok, event} <- SpanToEvent.span_to_event(span),
         {:ok, start} <- unix_start(span),
         {:ok, attributes} <- contract_attributes(span, event),
         {:ok, links} <- encode_links(span) do
      end_time = start + @end_delta

      {:ok,
       %Wire.Span{
         trace_id: trace_id(event.id),
         span_id: span_id(event.id),
         name: event.kind || "",
         kind: 2,
         start_time_unix_nano: start,
         end_time_unix_nano: end_time,
         attributes: attributes,
         links: links
       }}
    end
  end

  defp unix_start(%Span{start_time: nil}), do: {:error, :missing_start_time}

  defp unix_start(%Span{start_time: start_time}) do
    nanos = Int128.time_to_nanoseconds(start_time)

    if is_integer(nanos) and nanos >= 0 and nanos <= @u64_max - @end_delta do
      {:ok, nanos}
    else
      {:error, :start_time_not_unix_nanos}
    end
  end

  defp contract_attributes(%Span{} = span, %ProducerEvent{} = event) do
    with {:ok, id} <- required_text(event.id, :missing_event_id),
         {:ok, epoch, number} <- sequence_ints(event.sequence),
         {:ok, schema} <- required_text(event.schema_version, :missing_schema_version),
         {:ok, filter_epoch} <- filter_epoch(event.acknowledged_filter_epoch),
         {:ok, sensitivity} <- sensitivity_name(event.claimed_sensitivity),
         {:ok, capabilities} <- capability_names(event.claimed_capabilities || []),
         {:ok, domain} <- domain_attributes(span.attributes) do
      attributes = [
        kv(Span.attribute_event_id(), string_any(id)),
        kv(Span.attribute_sequence_epoch(), int_any(epoch)),
        kv(Span.attribute_sequence_number(), int_any(number)),
        kv(Span.attribute_schema_version(), string_any(schema)),
        kv(Span.attribute_filter_epoch(), int_any(filter_epoch)),
        kv(Span.attribute_sensitivity(), string_any(sensitivity))
      ]

      attributes =
        if capabilities == [] do
          attributes
        else
          attributes ++
            [kv(Span.attribute_capabilities(), array_any(Enum.map(capabilities, &string_any/1)))]
        end

      attributes =
        if is_binary(span.parent_event_id) and span.parent_event_id != "" do
          attributes ++ [kv(Span.attribute_parent_event_id(), string_any(span.parent_event_id))]
        else
          attributes
        end

      {:ok, attributes ++ domain}
    end
  end

  defp domain_attributes(nil), do: {:ok, []}

  defp domain_attributes(attributes) do
    Enum.reduce_while(attributes, {:ok, []}, fn {key, value}, {:ok, acc} ->
      cond do
        Span.reserved_attribute?(key) ->
          {:cont, {:ok, acc}}

        Span.sentinel_attribute?(key) ->
          {:halt, {:error, {:unknown_sentinel_attribute, key}}}

        true ->
          case tagged_to_any(key, value) do
            :skip -> {:cont, {:ok, acc}}
            {:ok, any} -> {:cont, {:ok, acc ++ [kv(key, any)]}}
            {:error, reason} -> {:halt, {:error, reason}}
          end
      end
    end)
  end

  defp encode_links(%Span{links: nil}), do: {:ok, []}

  defp encode_links(%Span{links: links}) do
    Enum.reduce_while(links, {:ok, []}, fn {event_id, _span_id}, {:ok, acc} ->
      link = %Wire.SpanLink{
        trace_id: trace_id(event_id),
        span_id: span_id("link:" <> event_id),
        attributes: [kv(Span.attribute_event_id(), string_any(event_id))]
      }

      {:cont, {:ok, acc ++ [link]}}
    end)
  end

  defp sequence_ints(%SequenceCoordinate{epoch: epoch, sequence: number}) do
    with {:ok, epoch} <- int64(epoch, :missing_sequence_epoch),
         {:ok, number} <- int64(number, :missing_sequence_number) do
      {:ok, epoch, number}
    end
  end

  defp sequence_ints(_), do: {:error, :missing_sequence}

  defp filter_epoch(epoch) when is_integer(epoch),
    do: int64(epoch, :missing_acknowledged_filter_epoch)

  defp filter_epoch(_), do: {:error, :missing_acknowledged_filter_epoch}

  defp sensitivity_name(atom) when is_atom(atom) do
    case Map.fetch(@sensitivities, atom) do
      {:ok, name} -> {:ok, name}
      :error -> {:error, :unspecified_sensitivity}
    end
  end

  defp sensitivity_name(_), do: {:error, :unspecified_sensitivity}

  defp capability_names(names) do
    Enum.reduce_while(names, {:ok, []}, fn name, {:ok, acc} ->
      case Map.fetch(@capabilities, name) do
        {:ok, text} -> {:cont, {:ok, acc ++ [text]}}
        :error -> {:halt, {:error, {:unknown_capability, name}}}
      end
    end)
  end

  defp required_text(text, _error) when is_binary(text) and text != "", do: {:ok, text}
  defp required_text(_, error), do: {:error, error}

  defp int64(value, _error) when is_integer(value) and value >= 0 and value <= @int64_max do
    {:ok, value}
  end

  defp int64(_, error), do: {:error, error}

  defp decode_spans([], _index, acc), do: {:ok, Enum.reverse(acc)}

  defp decode_spans([resource | rest], index, acc) do
    case decode_scope_spans(list(resource.scope_spans), index, acc) do
      {:ok, acc, index} -> decode_spans(rest, index, acc)
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_scope_spans([], index, acc), do: {:ok, acc, index}

  defp decode_scope_spans([scope | rest], index, acc) do
    case decode_span_list(list(scope.spans), index, acc) do
      {:ok, acc, index} -> decode_scope_spans(rest, index, acc)
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_span_list([], index, acc), do: {:ok, acc, index}

  defp decode_span_list([span | rest], index, acc) do
    case decode_span(span) do
      {:ok, event} -> decode_span_list(rest, index + 1, [event | acc])
      {:error, reason} -> {:error, {:rejected_span, index, reason}}
    end
  end

  defp decode_span(%Wire.Span{} = span) do
    with :ok <- ended?(span),
         :ok <- no_drops?(span),
         {:ok, fields, domain} <- read_span_fields(list(span.attributes)),
         {:ok, link_ids} <- link_event_ids(list(span.links)),
         :ok <- parent_present?(span, fields) do
      predecessors =
        case fields.parent do
          nil -> link_ids
          parent -> [parent | link_ids]
        end

      {:ok,
       %ProducerEvent{
         id: fields.id,
         sequence: %SequenceCoordinate{epoch: fields.epoch, sequence: fields.number},
         schema_version: fields.schema,
         acknowledged_filter_epoch: fields.filter_epoch,
         kind: span.name,
         occurrence_time: occurrence(span.start_time_unix_nano),
         attributes: SpanToEvent.map_attributes(domain),
         claimed_capabilities: fields.capabilities,
         claimed_sensitivity: fields.sensitivity,
         causal_predecessor_ids: predecessors
       }}
    end
  end

  defp ended?(%Wire.Span{end_time_unix_nano: nil}), do: {:error, :not_ended}
  defp ended?(%Wire.Span{end_time_unix_nano: 0}), do: {:error, :not_ended}

  defp ended?(%Wire.Span{start_time_unix_nano: start, end_time_unix_nano: finish}) do
    start = start || 0

    if finish < start do
      {:error, :end_before_start}
    else
      :ok
    end
  end

  defp no_drops?(%Wire.Span{} = span) do
    cond do
      positive?(span.dropped_attributes_count) ->
        {:error, :dropped_attributes}

      positive?(span.dropped_links_count) ->
        {:error, :dropped_links}

      positive?(span.dropped_events_count) ->
        {:error, :dropped_events}

      Enum.any?(list(span.links), &positive?(&1.dropped_attributes_count)) ->
        {:error, :dropped_link_attributes}

      Enum.any?(list(span.events), &positive?(&1.dropped_attributes_count)) ->
        {:error, :dropped_event_attributes}

      true ->
        :ok
    end
  end

  defp positive?(n) when is_integer(n) and n > 0, do: true
  defp positive?(_), do: false

  defp parent_present?(%Wire.Span{parent_span_id: id}, fields) do
    if parent_span_bytes?(id) and is_nil(fields.parent) do
      {:error, :missing_parent_event_id}
    else
      :ok
    end
  end

  defp parent_span_bytes?(nil), do: false
  defp parent_span_bytes?(<<>>), do: false

  defp parent_span_bytes?(bytes) when is_binary(bytes) do
    bytes != <<0::size(byte_size(bytes))-unit(8)>>
  end

  defp link_event_ids(links) do
    links
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {link, index}, {:ok, acc} ->
      case attribute_string(list(link.attributes), Span.attribute_event_id()) do
        {:ok, id} when id != "" -> {:cont, {:ok, acc ++ [id]}}
        _ -> {:halt, {:error, {:missing_link_event_id, index}}}
      end
    end)
  end

  defp read_span_fields(attributes) do
    Enum.reduce_while(attributes, {:ok, empty_fields(), []}, fn attr, {:ok, fields, domain} ->
      case take_field(attr, fields, domain) do
        {:ok, fields, domain} -> {:cont, {:ok, fields, domain}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, fields, domain} -> require_fields(fields, domain)
      other -> other
    end
  end

  defp empty_fields do
    %{
      id: nil,
      epoch: nil,
      number: nil,
      schema: nil,
      filter_epoch: nil,
      sensitivity: nil,
      capabilities: [],
      parent: nil
    }
  end

  defp take_field(%Wire.KeyValue{key: key} = attr, fields, domain) do
    cond do
      Span.reserved_attribute?(key) ->
        with {:ok, fields} <- assign_reserved(key, attr.value, fields) do
          {:ok, fields, domain}
        end

      Span.sentinel_attribute?(key) ->
        {:error, {:unknown_sentinel_attribute, key}}

      true ->
        case any_to_tagged(key, attr.value) do
          :skip -> {:ok, fields, domain}
          {:ok, tagged} -> {:ok, fields, domain ++ [{key, tagged}]}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp assign_reserved(key, value, fields) do
    cond do
      key == Span.attribute_event_id() ->
        with {:ok, text} <- any_string(value), do: {:ok, %{fields | id: text}}

      key == Span.attribute_parent_event_id() ->
        with {:ok, text} <- any_string(value), do: {:ok, %{fields | parent: text}}

      key == Span.attribute_sequence_epoch() ->
        with {:ok, n} <- any_uint(value), do: {:ok, %{fields | epoch: n}}

      key == Span.attribute_sequence_number() ->
        with {:ok, n} <- any_uint(value), do: {:ok, %{fields | number: n}}

      key == Span.attribute_schema_version() ->
        with {:ok, text} <- any_string(value), do: {:ok, %{fields | schema: text}}

      key == Span.attribute_filter_epoch() ->
        with {:ok, n} <- any_uint(value), do: {:ok, %{fields | filter_epoch: n}}

      key == Span.attribute_sensitivity() ->
        with {:ok, text} <- any_string(value),
             {:ok, atom} <- sensitivity_atom(text) do
          {:ok, %{fields | sensitivity: atom}}
        end

      key == Span.attribute_capabilities() ->
        with {:ok, names} <- capability_atoms(value), do: {:ok, %{fields | capabilities: names}}
    end
  end

  defp require_fields(fields, domain) do
    cond do
      fields.id in [nil, ""] -> {:error, :missing_event_id}
      is_nil(fields.epoch) -> {:error, :missing_sequence_epoch}
      is_nil(fields.number) -> {:error, :missing_sequence_number}
      fields.schema in [nil, ""] -> {:error, :missing_schema_version}
      is_nil(fields.filter_epoch) -> {:error, :missing_acknowledged_filter_epoch}
      is_nil(fields.sensitivity) -> {:error, :unspecified_sensitivity}
      true -> {:ok, fields, domain}
    end
  end

  defp sensitivity_atom(name) do
    case Enum.find(@sensitivities, fn {_atom, text} -> text == name end) do
      {atom, _} -> {:ok, atom}
      nil -> {:error, {:unknown_sensitivity, name}}
    end
  end

  defp capability_atoms(%Wire.AnyValue{value: {:array_value, %Wire.ArrayValue{values: values}}}) do
    Enum.reduce_while(list(values), {:ok, []}, fn value, {:ok, acc} ->
      with {:ok, text} <- any_string(value),
           {atom, _} <- Enum.find(@capabilities, fn {_atom, name} -> name == text end) do
        {:cont, {:ok, acc ++ [atom]}}
      else
        nil -> {:halt, {:error, :unknown_capability}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp capability_atoms(_), do: {:error, :invalid_capabilities}

  defp occurrence(start) do
    %OccurrenceTime{
      clock_domain_id: "unix",
      nanoseconds: Int128.from_int(start || 0),
      uncertainty_nanoseconds: 0
    }
  end

  defp encode_log(body, payload_type, opts) do
    body = IO.iodata_to_binary(body)
    attributes = [kv(@payload_type, string_any(payload_type))]

    attributes =
      case opts[:request_id] do
        nil -> attributes
        id -> attributes ++ [kv(@request_id, string_any(id))]
      end

    attributes =
      case opts[:source_handle] do
        nil -> attributes
        handle -> attributes ++ [kv(@source_handle, string_any(handle))]
      end

    request = %Wire.ExportLogsServiceRequest{
      resource_logs: [
        %Wire.ResourceLogs{
          scope_logs: [
            %Wire.ScopeLogs{
              log_records: [
                %Wire.LogRecord{
                  body: bytes_any(body),
                  attributes: attributes
                }
              ]
            }
          ]
        }
      ]
    }

    {:ok, iodata(request)}
  end

  defp require_specified(%DecideResponse{request_id: id}) when id in [nil, ""],
    do: {:error, :empty_request_id}

  defp require_specified(%DecideResponse{action: action, specifications: specs}) do
    with :ok <- specified_action(action) do
      Enum.reduce_while(list(specs), :ok, fn spec, :ok ->
        case specified_action(spec.action) do
          :ok -> {:cont, :ok}
          {:error, _} -> {:halt, {:error, :unspecified_specification_action}}
        end
      end)
    end
  end

  defp specified_action(action)
       when action in [:DECISION_ACTION_PERMIT, :DECISION_ACTION_DENY, :DECISION_ACTION_DEFER],
       do: :ok

  defp specified_action(_), do: {:error, :unspecified_action}

  defp read_records([], acc), do: {:ok, Enum.reverse(acc)}

  defp read_records([resource | rest], acc) do
    case read_scope_logs(list(resource.scope_logs), acc) do
      {:ok, acc} -> read_records(rest, acc)
      {:error, reason} -> {:error, reason}
    end
  end

  defp read_scope_logs([], acc), do: {:ok, acc}

  defp read_scope_logs([scope | rest], acc) do
    case read_log_list(list(scope.log_records), acc) do
      {:ok, acc} -> read_scope_logs(rest, acc)
      {:error, reason} -> {:error, reason}
    end
  end

  defp read_log_list([], acc), do: {:ok, acc}

  defp read_log_list([record | rest], acc) do
    case read_log(record) do
      {:ok, decoded} -> read_log_list(rest, [decoded | acc])
      {:error, reason} -> {:error, reason}
    end
  end

  defp read_log(%Wire.LogRecord{} = record) do
    if positive?(record.dropped_attributes_count) do
      {:error, :dropped_attributes}
    else
      with {:ok, fields} <- log_fields(list(record.attributes)),
           {:ok, body} <- log_body(record.body) do
        {:ok, Map.put(fields, :body, body)}
      end
    end
  end

  defp log_fields(attributes) do
    Enum.reduce_while(
      attributes,
      {:ok, %{payload_type: nil, request_id: nil, source_handle: nil}},
      fn
        %Wire.KeyValue{key: key, value: value}, {:ok, fields} ->
          cond do
            key == @payload_type ->
              take_log_string(value, fields, :payload_type)

            key == @request_id ->
              take_log_string(value, fields, :request_id)

            key == @source_handle ->
              take_log_string(value, fields, :source_handle)

            Span.sentinel_attribute?(key) ->
              {:halt, {:error, {:unknown_sentinel_attribute, key}}}

            true ->
              case any_to_tagged(key, value) do
                {:error, reason} -> {:halt, {:error, reason}}
                _ -> {:cont, {:ok, fields}}
              end
          end
      end
    )
  end

  defp take_log_string(value, fields, key) do
    case any_string(value) do
      {:ok, text} -> {:cont, {:ok, Map.put(fields, key, text)}}
      {:error, reason} -> {:halt, {:error, reason}}
    end
  end

  defp log_body(%Wire.AnyValue{value: {:bytes_value, bytes}}) when is_binary(bytes),
    do: {:ok, bytes}

  defp log_body(_), do: {:error, :body_not_bytes}

  defp decode_probe_records([], acc), do: {:ok, Enum.reverse(acc)}

  defp decode_probe_records([record | rest], acc) do
    case classify(record) do
      {:ok, payload} -> decode_probe_records(rest, [payload | acc])
      {:error, reason} -> {:error, reason}
    end
  end

  defp classify(%{payload_type: @verdict, request_id: nil, source_handle: nil, body: body}) do
    with {:ok, verdict} <- decode_message(body, Verdict), do: {:ok, {:verdict, verdict}}
  end

  defp classify(%{payload_type: @event_filter, request_id: nil, source_handle: nil, body: body}) do
    with {:ok, filter} <- decode_message(body, EventFilter), do: {:ok, {:event_filter, filter}}
  end

  defp classify(%{payload_type: @decide, source_handle: handle}) when not is_nil(handle) do
    {:error, {:unknown_sentinel_attribute, @source_handle}}
  end

  defp classify(%{payload_type: @decide, request_id: request_id, body: body})
       when is_binary(request_id) and request_id != "" do
    with {:ok, response} <- decode_message(body, DecideResponse),
         :ok <- request_matches(response, request_id),
         :ok <- require_specified(response) do
      {:ok, {:decide, response}}
    end
  end

  defp classify(%{payload_type: @decide}), do: {:error, :missing_request_id}
  defp classify(%{payload_type: type}), do: {:error, {:rejected_payload, type}}

  defp request_matches(%DecideResponse{request_id: id}, id) when id != "", do: :ok
  defp request_matches(_, _), do: {:error, :request_id_mismatch}

  defp reject_wrapped(<<0x1F, 0x8B, _::binary>>), do: {:error, :compressed_gzip}

  defp reject_wrapped(bytes) do
    case skip_space(bytes) do
      <<"{", _::binary>> -> {:error, :json_payload}
      <<"[", _::binary>> -> {:error, :json_payload}
      <<0xEF, 0xBB, 0xBF, rest::binary>> -> reject_wrapped(rest)
      _ -> :ok
    end
  end

  defp skip_space(<<c, rest::binary>>) when c in [?\s, ?\t, ?\n, ?\r], do: skip_space(rest)
  defp skip_space(bytes), do: bytes

  defp decode_message(bytes, module) do
    {:ok, Protobuf.decode(bytes, module)}
  rescue
    error -> {:error, {:malformed_protobuf, Exception.message(error)}}
  end

  defp list(nil), do: []
  defp list(items) when is_list(items), do: items

  defp iodata(message), do: IO.iodata_to_binary(Protobuf.encode(message))

  defp kv(key, %Wire.AnyValue{} = value), do: %Wire.KeyValue{key: key, value: value}

  defp string_any(text), do: %Wire.AnyValue{value: {:string_value, text}}
  defp int_any(n), do: %Wire.AnyValue{value: {:int_value, n}}
  defp bytes_any(bytes), do: %Wire.AnyValue{value: {:bytes_value, bytes}}

  defp array_any(values),
    do: %Wire.AnyValue{value: {:array_value, %Wire.ArrayValue{values: values}}}

  defp tagged_to_any(_key, nil), do: :skip
  defp tagged_to_any(_key, {:string, text}) when is_binary(text), do: {:ok, string_any(text)}

  defp tagged_to_any(_key, {:bool, flag}) when is_boolean(flag),
    do: {:ok, %Wire.AnyValue{value: {:bool_value, flag}}}

  defp tagged_to_any(_key, {:int, n}) when is_integer(n) and n >= @int64_min and n <= @int64_max do
    {:ok, int_any(n)}
  end

  defp tagged_to_any(key, {:int, _}), do: {:error, {:int_out_of_range, key}}

  defp tagged_to_any(key, {:double, f}) when is_float(f) do
    if non_finite?(f),
      do: {:error, {:non_finite_double, key}},
      else: {:ok, %Wire.AnyValue{value: {:double_value, f}}}
  end

  defp tagged_to_any(_key, {:bytes, bytes}) when is_binary(bytes), do: {:ok, bytes_any(bytes)}

  defp tagged_to_any(key, {:array, values}) when is_list(values) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, acc} ->
      case tagged_to_any(key, value) do
        :skip -> {:cont, {:ok, acc}}
        {:ok, any} -> {:cont, {:ok, acc ++ [any]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, anys} -> {:ok, array_any(anys)}
      error -> error
    end
  end

  defp tagged_to_any(_key, {:map, entries}) when is_list(entries) do
    Enum.reduce_while(entries, {:ok, []}, fn {inner, value}, {:ok, acc} ->
      if Span.sentinel_attribute?(inner) do
        {:halt, {:error, {:unknown_sentinel_attribute, inner}}}
      else
        case tagged_to_any(inner, value) do
          :skip -> {:cont, {:ok, acc}}
          {:ok, any} -> {:cont, {:ok, acc ++ [kv(inner, any)]}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end
    end)
    |> case do
      {:ok, pairs} ->
        {:ok, %Wire.AnyValue{value: {:kvlist_value, %Wire.KeyValueList{values: pairs}}}}

      error ->
        error
    end
  end

  defp tagged_to_any(key, _), do: {:error, {:unsupported_attribute, key}}

  defp any_to_tagged(_key, nil), do: :skip
  defp any_to_tagged(_key, %Wire.AnyValue{value: nil}), do: :skip
  defp any_to_tagged(_key, %Wire.AnyValue{value: {:string_value, text}}), do: {:ok, {:string, text}}
  defp any_to_tagged(_key, %Wire.AnyValue{value: {:bool_value, flag}}), do: {:ok, {:bool, flag}}
  defp any_to_tagged(_key, %Wire.AnyValue{value: {:int_value, n}}), do: {:ok, {:int, n}}

  defp any_to_tagged(key, %Wire.AnyValue{value: {:double_value, f}}) do
    if non_finite?(f), do: {:error, {:non_finite_double, key}}, else: {:ok, {:double, f}}
  end

  defp any_to_tagged(_key, %Wire.AnyValue{value: {:bytes_value, bytes}}), do: {:ok, {:bytes, bytes}}

  defp any_to_tagged(key, %Wire.AnyValue{value: {:array_value, %Wire.ArrayValue{values: values}}}) do
    Enum.reduce_while(list(values), {:ok, []}, fn value, {:ok, acc} ->
      case any_to_tagged(key, value) do
        :skip -> {:cont, {:ok, acc}}
        {:ok, tagged} -> {:cont, {:ok, acc ++ [tagged]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, tagged} -> {:ok, {:array, tagged}}
      error -> error
    end
  end

  defp any_to_tagged(_key, %Wire.AnyValue{
         value: {:kvlist_value, %Wire.KeyValueList{values: values}}
       }) do
    Enum.reduce_while(list(values), {:ok, []}, fn %Wire.KeyValue{key: inner, value: value},
                                                  {:ok, acc} ->
      if Span.sentinel_attribute?(inner) do
        {:halt, {:error, {:unknown_sentinel_attribute, inner}}}
      else
        case any_to_tagged(inner, value) do
          :skip -> {:cont, {:ok, acc}}
          {:ok, tagged} -> {:cont, {:ok, acc ++ [{inner, tagged}]}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end
    end)
    |> case do
      {:ok, entries} -> {:ok, {:map, entries}}
      error -> error
    end
  end

  defp any_string(%Wire.AnyValue{value: {:string_value, text}}) when is_binary(text),
    do: {:ok, text}

  defp any_string(_), do: {:error, :invalid_string}

  defp any_uint(%Wire.AnyValue{value: {:int_value, n}}) when is_integer(n) and n >= 0, do: {:ok, n}
  defp any_uint(_), do: {:error, :invalid_int}

  defp attribute_string(attributes, key) do
    Enum.find_value(attributes, :error, fn
      %Wire.KeyValue{key: ^key, value: value} -> any_string(value)
      _ -> nil
    end)
    |> case do
      {:ok, text} -> {:ok, text}
      _ -> :error
    end
  end

  defp trace_id(event_id), do: :crypto.hash(:md5, event_id)

  defp span_id(event_id) do
    <<id::binary-size(8), _::binary>> = :crypto.hash(:sha256, event_id)
    id
  end

  defp non_finite?(f) when is_float(f) do
    match?(<<_::1, 2047::11, _::52>>, <<f::float-64>>)
  end

  defp non_finite?(_), do: true
end
