defmodule Sentinel.Probe.SDK.Emission.Span do
  @moduledoc """
  The internal representation of an ended OTel span that `SpanToEvent` consumes.

  Elixir analog of Go's `sdktrace.ReadOnlySpan` + `SpanConversion`, collapsed
  into one struct. The struct IS the fake: tests construct it directly (mirroring
  Go's `tracetest.SpanStub{}.Snapshot()`), and a custom `:otel_span_processor`
  `on_end/2` callback (see `Sentinel.Probe.SDK.Emission.Processor`) populates it
  from the OTel Erlang SDK's `#span{}` record at runtime. It is deliberately NOT
  a behaviour or protocol — a plain struct keeps the conversion pure and the
  tests hermetic, with no live TracerProvider required (ADR-0002: the host owns
  export).

  ## Attribute values

  OTel attribute values are tagged, because Elixir binaries are ambiguous
  between `string_value` and `bytes_value` and because the oneof arm must be
  chosen explicitly (divergence D14: type-directed, so `3.0` stays a double):

    * `{:string, s}`
    * `{:bool, b}`
    * `{:int, i}`        → `integer_value` (sint64)
    * `{:double, f}`     → `double_value`
    * `{:bytes, b}`      → `bytes_value`
    * `{:array, [values]}` → `array_value` (members mapped recursively)
    * `{:map, [{k, v}]}`  → `map_value`

  `nil` members are skipped, as the reference skips null/undefined.
  """

  # Reserved attribute keys. They are the #398 span contract, never domain
  # attributes. A span id is not one of them.
  @attribute_event_id "sentinel.event.id"
  @attribute_parent_event_id "sentinel.parent.event.id"
  @attribute_sequence_epoch "sentinel.sequence.epoch"
  @attribute_sequence_number "sentinel.sequence.number"
  @attribute_schema_version "sentinel.schema.version"
  @attribute_filter_epoch "sentinel.acknowledged_filter_epoch"
  @attribute_sensitivity "sentinel.claimed_sensitivity"
  @attribute_capabilities "sentinel.claimed_capabilities"

  @reserved_attribute_keys [
    @attribute_event_id,
    @attribute_parent_event_id,
    @attribute_sequence_epoch,
    @attribute_sequence_number,
    @attribute_schema_version,
    @attribute_filter_epoch,
    @attribute_sensitivity,
    @attribute_capabilities
  ]

  # Kept so existing span fixtures still construct. A missing event id fails
  # conversion; these labels are not a substitute predecessor.
  @malformed_link_source_parent "parent"
  @malformed_link_source_link "link"

  @doc false
  def attribute_event_id, do: @attribute_event_id
  def attribute_parent_event_id, do: @attribute_parent_event_id
  def attribute_sequence_epoch, do: @attribute_sequence_epoch
  def attribute_sequence_number, do: @attribute_sequence_number
  def attribute_schema_version, do: @attribute_schema_version
  def attribute_filter_epoch, do: @attribute_filter_epoch
  def attribute_sensitivity, do: @attribute_sensitivity
  def attribute_capabilities, do: @attribute_capabilities
  def reserved_attribute_keys, do: @reserved_attribute_keys
  def reserved_attribute?(key) when is_binary(key), do: key in @reserved_attribute_keys
  def reserved_attribute?(_), do: false
  def sentinel_attribute?(<<"sentinel.", _::binary>>), do: true
  def sentinel_attribute?(_), do: false
  def malformed_link_source_parent, do: @malformed_link_source_parent
  def malformed_link_source_link, do: @malformed_link_source_link

  defstruct [
    :event_id,
    :sequence,
    :schema_version,
    :acknowledged_epoch,
    :claimed_capabilities,
    :claimed_sensitivity,
    :name,
    :start_time,
    :attributes,
    :parent_event_id,
    :parent_span_id,
    :links,
    :on_malformed_link
  ]

  @type attribute_value ::
          {:string, String.t()}
          | {:bool, boolean()}
          | {:int, integer()}
          | {:double, float()}
          | {:bytes, binary()}
          | {:array, [attribute_value()]}
          | {:map, [{String.t(), attribute_value()}]}

  @type start_time ::
          {seconds :: integer(), nanos :: integer()}
          | DateTime.t()
          | integer()
          | nil

  @type link :: {event_id :: String.t() | nil, span_id :: String.t()}

  @type t :: %__MODULE__{
          event_id: String.t() | nil,
          sequence: Sentinel.Model.V1.SequenceCoordinate.t() | nil,
          schema_version: String.t() | nil,
          acknowledged_epoch: non_neg_integer() | nil,
          claimed_capabilities: [atom()] | nil,
          claimed_sensitivity: atom() | nil,
          name: String.t() | nil,
          start_time: start_time(),
          attributes: [{String.t(), attribute_value()}] | nil,
          parent_event_id: String.t() | nil,
          parent_span_id: String.t() | nil,
          links: [link()] | nil,
          on_malformed_link: (String.t(), String.t() -> term()) | nil
        }
end
