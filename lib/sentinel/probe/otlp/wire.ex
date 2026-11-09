defmodule Sentinel.Probe.SDK.Otlp.Wire do
  @moduledoc """
  OTLP/HTTP protobuf messages the Probe encodes and decodes.

  Field numbers match the prost messages in `gen/rust/otlp`. They are the
  OpenTelemetry collector trace and log requests, not Sentinel's own schema.
  """
end

defmodule Sentinel.Probe.SDK.Otlp.Wire.AnyValue do
  @moduledoc false
  use Protobuf, syntax: :proto3

  oneof(:value, 0)
  field(:string_value, 1, type: :string, oneof: 0)
  field(:bool_value, 2, type: :bool, oneof: 0)
  field(:int_value, 3, type: :int64, oneof: 0)
  field(:double_value, 4, type: :double, oneof: 0)
  field(:array_value, 5, type: Sentinel.Probe.SDK.Otlp.Wire.ArrayValue, oneof: 0)
  field(:kvlist_value, 6, type: Sentinel.Probe.SDK.Otlp.Wire.KeyValueList, oneof: 0)
  field(:bytes_value, 7, type: :bytes, oneof: 0)
end

defmodule Sentinel.Probe.SDK.Otlp.Wire.ArrayValue do
  @moduledoc false
  use Protobuf, syntax: :proto3

  field(:values, 1, repeated: true, type: Sentinel.Probe.SDK.Otlp.Wire.AnyValue)
end

defmodule Sentinel.Probe.SDK.Otlp.Wire.KeyValueList do
  @moduledoc false
  use Protobuf, syntax: :proto3

  field(:values, 1, repeated: true, type: Sentinel.Probe.SDK.Otlp.Wire.KeyValue)
end

defmodule Sentinel.Probe.SDK.Otlp.Wire.KeyValue do
  @moduledoc false
  use Protobuf, syntax: :proto3

  field(:key, 1, type: :string)
  field(:value, 2, type: Sentinel.Probe.SDK.Otlp.Wire.AnyValue)
end

defmodule Sentinel.Probe.SDK.Otlp.Wire.InstrumentationScope do
  @moduledoc false
  use Protobuf, syntax: :proto3

  field(:name, 1, type: :string)
  field(:version, 2, type: :string)
  field(:attributes, 3, repeated: true, type: Sentinel.Probe.SDK.Otlp.Wire.KeyValue)
  field(:dropped_attributes_count, 4, type: :uint32)
end

defmodule Sentinel.Probe.SDK.Otlp.Wire.Resource do
  @moduledoc false
  use Protobuf, syntax: :proto3

  field(:attributes, 1, repeated: true, type: Sentinel.Probe.SDK.Otlp.Wire.KeyValue)
  field(:dropped_attributes_count, 2, type: :uint32)
end

defmodule Sentinel.Probe.SDK.Otlp.Wire.SpanEvent do
  @moduledoc false
  use Protobuf, syntax: :proto3

  field(:time_unix_nano, 1, type: :fixed64)
  field(:name, 2, type: :string)
  field(:attributes, 3, repeated: true, type: Sentinel.Probe.SDK.Otlp.Wire.KeyValue)
  field(:dropped_attributes_count, 4, type: :uint32)
end

defmodule Sentinel.Probe.SDK.Otlp.Wire.SpanLink do
  @moduledoc false
  use Protobuf, syntax: :proto3

  field(:trace_id, 1, type: :bytes)
  field(:span_id, 2, type: :bytes)
  field(:trace_state, 3, type: :string)
  field(:attributes, 4, repeated: true, type: Sentinel.Probe.SDK.Otlp.Wire.KeyValue)
  field(:dropped_attributes_count, 5, type: :uint32)
  field(:flags, 6, type: :fixed32)
end

defmodule Sentinel.Probe.SDK.Otlp.Wire.Span do
  @moduledoc false
  use Protobuf, syntax: :proto3

  field(:trace_id, 1, type: :bytes)
  field(:span_id, 2, type: :bytes)
  field(:trace_state, 3, type: :string)
  field(:parent_span_id, 4, type: :bytes)
  field(:name, 5, type: :string)
  field(:kind, 6, type: :int32)
  field(:start_time_unix_nano, 7, type: :fixed64)
  field(:end_time_unix_nano, 8, type: :fixed64)
  field(:attributes, 9, repeated: true, type: Sentinel.Probe.SDK.Otlp.Wire.KeyValue)
  field(:dropped_attributes_count, 10, type: :uint32)
  field(:events, 11, repeated: true, type: Sentinel.Probe.SDK.Otlp.Wire.SpanEvent)
  field(:dropped_events_count, 12, type: :uint32)
  field(:links, 13, repeated: true, type: Sentinel.Probe.SDK.Otlp.Wire.SpanLink)
  field(:dropped_links_count, 14, type: :uint32)
  field(:flags, 16, type: :fixed32)
end

defmodule Sentinel.Probe.SDK.Otlp.Wire.ScopeSpans do
  @moduledoc false
  use Protobuf, syntax: :proto3

  field(:scope, 1, type: Sentinel.Probe.SDK.Otlp.Wire.InstrumentationScope)
  field(:spans, 2, repeated: true, type: Sentinel.Probe.SDK.Otlp.Wire.Span)
  field(:schema_url, 3, type: :string)
end

defmodule Sentinel.Probe.SDK.Otlp.Wire.ResourceSpans do
  @moduledoc false
  use Protobuf, syntax: :proto3

  field(:resource, 1, type: Sentinel.Probe.SDK.Otlp.Wire.Resource)
  field(:scope_spans, 2, repeated: true, type: Sentinel.Probe.SDK.Otlp.Wire.ScopeSpans)
  field(:schema_url, 3, type: :string)
end

defmodule Sentinel.Probe.SDK.Otlp.Wire.ExportTraceServiceRequest do
  @moduledoc false
  use Protobuf, syntax: :proto3

  field(:resource_spans, 1, repeated: true, type: Sentinel.Probe.SDK.Otlp.Wire.ResourceSpans)
end

defmodule Sentinel.Probe.SDK.Otlp.Wire.LogRecord do
  @moduledoc false
  use Protobuf, syntax: :proto3

  field(:time_unix_nano, 1, type: :fixed64)
  field(:severity_number, 2, type: :int32)
  field(:severity_text, 3, type: :string)
  field(:body, 5, type: Sentinel.Probe.SDK.Otlp.Wire.AnyValue)
  field(:attributes, 6, repeated: true, type: Sentinel.Probe.SDK.Otlp.Wire.KeyValue)
  field(:dropped_attributes_count, 7, type: :uint32)
  field(:flags, 8, type: :fixed32)
  field(:trace_id, 9, type: :bytes)
  field(:span_id, 10, type: :bytes)
  field(:observed_time_unix_nano, 11, type: :fixed64)
  field(:event_name, 12, type: :string)
end

defmodule Sentinel.Probe.SDK.Otlp.Wire.ScopeLogs do
  @moduledoc false
  use Protobuf, syntax: :proto3

  field(:scope, 1, type: Sentinel.Probe.SDK.Otlp.Wire.InstrumentationScope)
  field(:log_records, 2, repeated: true, type: Sentinel.Probe.SDK.Otlp.Wire.LogRecord)
  field(:schema_url, 3, type: :string)
end

defmodule Sentinel.Probe.SDK.Otlp.Wire.ResourceLogs do
  @moduledoc false
  use Protobuf, syntax: :proto3

  field(:resource, 1, type: Sentinel.Probe.SDK.Otlp.Wire.Resource)
  field(:scope_logs, 2, repeated: true, type: Sentinel.Probe.SDK.Otlp.Wire.ScopeLogs)
  field(:schema_url, 3, type: :string)
end

defmodule Sentinel.Probe.SDK.Otlp.Wire.ExportLogsServiceRequest do
  @moduledoc false
  use Protobuf, syntax: :proto3

  field(:resource_logs, 1, repeated: true, type: Sentinel.Probe.SDK.Otlp.Wire.ResourceLogs)
end
