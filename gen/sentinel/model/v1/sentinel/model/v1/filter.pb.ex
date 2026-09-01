defmodule Sentinel.Model.V1.FailMode do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "sentinel.model.v1.FailMode",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :FAIL_MODE_UNSPECIFIED, 0
  field :FAIL_MODE_OPEN, 1
  field :FAIL_MODE_CLOSED, 2
end

defmodule Sentinel.Model.V1.EvaluationMode do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "sentinel.model.v1.EvaluationMode",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :EVALUATION_MODE_UNSPECIFIED, 0
  field :EVALUATION_MODE_SHADOW, 1
  field :EVALUATION_MODE_DETECT, 2
  field :EVALUATION_MODE_ENFORCE, 3
end

defmodule Sentinel.Model.V1.Readiness do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "sentinel.model.v1.Readiness",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :READINESS_UNSPECIFIED, 0
  field :READINESS_WARMING, 1
  field :READINESS_ACTIVE, 2
end

defmodule Sentinel.Model.V1.DeliveryMode do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "sentinel.model.v1.DeliveryMode",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :DELIVERY_MODE_UNSPECIFIED, 0
  field :DELIVERY_MODE_SHIP_ASYNC, 1
  field :DELIVERY_MODE_ASK_AND_BLOCK, 2
end

defmodule Sentinel.Model.V1.EventMatch do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.EventMatch",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :event_kinds, 1, repeated: true, type: :string, json_name: "eventKinds"

  field :projected_attribute_keys, 2,
    repeated: true,
    type: :string,
    json_name: "projectedAttributeKeys"

  field :delivery_mode, 3,
    type: Sentinel.Model.V1.DeliveryMode,
    json_name: "deliveryMode",
    enum: true
end

defmodule Sentinel.Model.V1.SpecificationFilter do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.SpecificationFilter",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :specification_id, 1, type: :string, json_name: "specificationId"
  field :specification_version, 2, type: :string, json_name: "specificationVersion"
  field :event_match, 3, type: Sentinel.Model.V1.EventMatch, json_name: "eventMatch"
  field :fail_mode, 4, type: Sentinel.Model.V1.FailMode, json_name: "failMode", enum: true

  field :latency_budget_nanoseconds, 5,
    proto3_optional: true,
    type: :uint64,
    json_name: "latencyBudgetNanoseconds"

  field :evaluation_mode, 6,
    type: Sentinel.Model.V1.EvaluationMode,
    json_name: "evaluationMode",
    enum: true

  field :readiness, 7, type: Sentinel.Model.V1.Readiness, enum: true

  field :target_filter_epoch, 8,
    proto3_optional: true,
    type: :uint64,
    json_name: "targetFilterEpoch"

  field :required_sources, 9,
    repeated: true,
    type: Sentinel.Model.V1.SourceIdentity,
    json_name: "requiredSources"

  field :activation_watermarks, 10,
    repeated: true,
    type: Sentinel.Model.V1.Watermark,
    json_name: "activationWatermarks"

  field :enforcement_gates, 11,
    type: Sentinel.Model.V1.EnforcementGateEvidence,
    json_name: "enforcementGates"
end

defmodule Sentinel.Model.V1.EnforcementGateEvidence do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.EnforcementGateEvidence",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :safety_property, 1, type: :bool, json_name: "safetyProperty"
  field :ask_and_block, 2, type: :bool, json_name: "askAndBlock"
  field :hot_path_only, 3, type: :bool, json_name: "hotPathOnly"
  field :anchor_tier, 4, type: :bool, json_name: "anchorTier"

  field :shadow_and_adversarial_validation_passed, 5,
    type: :bool,
    json_name: "shadowAndAdversarialValidationPassed"
end

defmodule Sentinel.Model.V1.BaselineEvidenceFilter do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.BaselineEvidenceFilter",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :source_health, 1, type: :bool, json_name: "sourceHealth"
  field :completeness_signals, 2, type: :bool, json_name: "completenessSignals"
end

defmodule Sentinel.Model.V1.EventFilter do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.EventFilter",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :epoch, 1, proto3_optional: true, type: :uint64
  field :specifications, 2, repeated: true, type: Sentinel.Model.V1.SpecificationFilter

  field :baseline_evidence, 3,
    type: Sentinel.Model.V1.BaselineEvidenceFilter,
    json_name: "baselineEvidence"
end
