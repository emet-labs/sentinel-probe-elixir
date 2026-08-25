defmodule Sentinel.Probe.SDK.TestHelpers do
  @moduledoc false
  # Shared builders for the BEAM Probe SDK test suite, mirroring the per-package
  # helpers in the Go analog's *_test.go files (makeEvent/makeSpec/makeFilter).

  alias Sentinel.Model.V1.{
    AttributeArray,
    AttributeEntry,
    AttributeMap,
    AttributeValue,
    DeliveryMode,
    EventFilter,
    EventMatch,
    FailMode,
    ProducerEvent,
    SequenceCoordinate,
    SpecificationFilter
  }

  alias Sentinel.Probe.V1.{DecideRequest, DecideResponse, SpecificationDecision}

  def test_kind, do: "transfer.initiated"
  def test_epoch, do: 5

  def test_options do
    %Sentinel.Probe.SDK.Enforcement.Gate.Options{
      source_handle: "gateway.tool-calls",
      request_id: "req-1",
      idempotency_key: "idem-1"
    }
  end

  def make_event(kind) do
    %ProducerEvent{id: "evt-1", kind: kind, schema_version: "sentinel.model.v1"}
  end

  def make_event(kind, attrs) do
    %ProducerEvent{
      id: "evt-1",
      kind: kind,
      schema_version: "sentinel.model.v1",
      attributes: attrs
    }
  end

  def make_spec(specification_id, kinds, fail_mode, delivery_mode) do
    %SpecificationFilter{
      specification_id: specification_id,
      specification_version: "1.0.0",
      event_match: %EventMatch{
        event_kinds: kinds,
        delivery_mode: delivery_mode
      },
      fail_mode: fail_mode,
      evaluation_mode: :EVALUATION_MODE_ENFORCE,
      readiness: :READINESS_ACTIVE,
      latency_budget_nanoseconds: 10_000
    }
  end

  def ask_and_block_spec,
    do: make_spec("spec-1", [test_kind()], :FAIL_MODE_OPEN, :DELIVERY_MODE_ASK_AND_BLOCK)

  def closed_ask_and_block_spec,
    do: make_spec("spec-1", [test_kind()], :FAIL_MODE_CLOSED, :DELIVERY_MODE_ASK_AND_BLOCK)

  def ship_async_spec,
    do: make_spec("spec-1", [test_kind()], :FAIL_MODE_OPEN, :DELIVERY_MODE_SHIP_ASYNC)

  def make_filter(epoch, specs) do
    %EventFilter{epoch: epoch, specifications: specs}
  end

  def make_deps(mock, now_ns, accepted) do
    accepted = accepted || fn _spec -> :FAIL_MODE_OPEN end

    %Sentinel.Probe.SDK.Enforcement.Gate.Deps{
      decide: fn request -> Sentinel.Probe.SDK.MockDecider.decide(mock, request) end,
      now_monotonic_ns: fn -> now_ns end,
      accepted_fail_mode_for: accepted
    }
  end

  def always_closed(_spec), do: :FAIL_MODE_CLOSED
  def always_open(_spec), do: :FAIL_MODE_OPEN

  def make_response(action, decisions \\ []) do
    %DecideResponse{request_id: "req-1", action: action, specifications: decisions}
  end

  def make_decision(specification_id, action, unresolved_reason \\ nil) do
    %SpecificationDecision{
      specification_id: specification_id,
      specification_version: "1.0.0",
      action: action,
      unresolved_reason: unresolved_reason
    }
  end

  # Attribute builders — the oneof arm is the raw generated form so tests assert
  # against the wire shape directly.
  def string_attr(k, v),
    do: %AttributeEntry{key: k, value: %AttributeValue{value: {:string_value, v}}}

  def bool_attr(k, v), do: %AttributeEntry{key: k, value: %AttributeValue{value: {:bool_value, v}}}

  def int_attr(k, v),
    do: %AttributeEntry{key: k, value: %AttributeValue{value: {:integer_value, v}}}

  def double_attr(k, v),
    do: %AttributeEntry{key: k, value: %AttributeValue{value: {:double_value, v}}}

  def bytes_attr(k, v),
    do: %AttributeEntry{key: k, value: %AttributeValue{value: {:bytes_value, v}}}

  def array_attr(k, members),
    do: %AttributeEntry{
      key: k,
      value: %AttributeValue{value: {:array_value, %AttributeArray{values: members}}}
    }

  def map_attr(k, entries),
    do: %AttributeEntry{
      key: k,
      value: %AttributeValue{value: {:map_value, %AttributeMap{entries: entries}}}
    }

  def sequence(epoch, seq), do: %SequenceCoordinate{epoch: epoch, sequence: seq}
end
