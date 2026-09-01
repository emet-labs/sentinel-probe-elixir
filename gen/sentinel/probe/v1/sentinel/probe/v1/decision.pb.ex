defmodule Sentinel.Probe.V1.DecisionAction do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "sentinel.probe.v1.DecisionAction",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :DECISION_ACTION_UNSPECIFIED, 0
  field :DECISION_ACTION_PERMIT, 1
  field :DECISION_ACTION_DENY, 2
  field :DECISION_ACTION_DEFER, 3
end

defmodule Sentinel.Probe.V1.UnresolvedReason do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "sentinel.probe.v1.UnresolvedReason",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :UNRESOLVED_REASON_UNSPECIFIED, 0
  field :UNRESOLVED_REASON_PREFIX_UNCERTAINTY, 1
  field :UNRESOLVED_REASON_OBSERVATIONAL_UNCERTAINTY, 2
  field :UNRESOLVED_REASON_EPISTEMIC_UNCERTAINTY, 3
  field :UNRESOLVED_REASON_EVIDENCE_GAP, 4
  field :UNRESOLVED_REASON_TIMEOUT, 5
end

defmodule Sentinel.Probe.V1.DecideRequest do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.probe.v1.DecideRequest",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :request_id, 1, type: :string, json_name: "requestId"
  field :idempotency_key, 2, type: :string, json_name: "idempotencyKey"
  field :source_handle, 3, type: :string, json_name: "sourceHandle"
  field :filter_epoch, 4, proto3_optional: true, type: :uint64, json_name: "filterEpoch"
  field :producer_event, 5, type: Sentinel.Model.V1.ProducerEvent, json_name: "producerEvent"

  field :remaining_transport_budget_nanoseconds, 6,
    proto3_optional: true,
    type: :uint64,
    json_name: "remainingTransportBudgetNanoseconds"
end

defmodule Sentinel.Probe.V1.SpecificationDecision do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.probe.v1.SpecificationDecision",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :specification_id, 1, type: :string, json_name: "specificationId"
  field :specification_version, 2, type: :string, json_name: "specificationVersion"
  field :action, 3, type: Sentinel.Probe.V1.DecisionAction, enum: true
  field :fail_mode, 4, type: Sentinel.Model.V1.FailMode, json_name: "failMode", enum: true

  field :unresolved_reason, 5,
    proto3_optional: true,
    type: Sentinel.Probe.V1.UnresolvedReason,
    json_name: "unresolvedReason",
    enum: true
end

defmodule Sentinel.Probe.V1.DecideResponse do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.probe.v1.DecideResponse",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :request_id, 1, type: :string, json_name: "requestId"
  field :action, 2, type: Sentinel.Probe.V1.DecisionAction, enum: true
  field :specifications, 3, repeated: true, type: Sentinel.Probe.V1.SpecificationDecision
end
