defmodule Sentinel.Model.V1.SourceCapability do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "sentinel.model.v1.SourceCapability",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :SOURCE_CAPABILITY_UNSPECIFIED, 0
  field :SOURCE_CAPABILITY_OBSERVE_BEFORE_EFFECT, 1
  field :SOURCE_CAPABILITY_CAUSAL_EDGES, 2
  field :SOURCE_CAPABILITY_COMPLETENESS_SIGNALS, 3
  field :SOURCE_CAPABILITY_BOUNDED_CLOCK_UNCERTAINTY, 4
end

defmodule Sentinel.Model.V1.Sensitivity do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "sentinel.model.v1.Sensitivity",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :SENSITIVITY_UNSPECIFIED, 0
  field :SENSITIVITY_PUBLIC, 1
  field :SENSITIVITY_INTERNAL, 2
  field :SENSITIVITY_CONFIDENTIAL, 3
  field :SENSITIVITY_RESTRICTED, 4
end

defmodule Sentinel.Model.V1.DataClass do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "sentinel.model.v1.DataClass",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :DATA_CLASS_UNSPECIFIED, 0
  field :DATA_CLASS_TENANT, 1
  field :DATA_CLASS_CORPUS, 2
end

defmodule Sentinel.Model.V1.SourceTier do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "sentinel.model.v1.SourceTier",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :SOURCE_TIER_UNSPECIFIED, 0
  field :SOURCE_TIER_ANCHOR, 1
  field :SOURCE_TIER_CONTRIBUTING, 2
end

defmodule Sentinel.Model.V1.Integrity do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "sentinel.model.v1.Integrity",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :INTEGRITY_UNSPECIFIED, 0
  field :INTEGRITY_UNVERIFIED, 1
  field :INTEGRITY_AUTHENTICATED, 2
  field :INTEGRITY_TAMPER_EVIDENT, 3
end

defmodule Sentinel.Model.V1.SourceActivityState do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "sentinel.model.v1.SourceActivityState",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :SOURCE_ACTIVITY_STATE_UNSPECIFIED, 0
  field :SOURCE_ACTIVITY_STATE_ACTIVE, 1
  field :SOURCE_ACTIVITY_STATE_INTENTIONALLY_IDLE, 2
end

defmodule Sentinel.Model.V1.Int128 do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.Int128",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :high, 1, type: :sfixed64
  field :low, 2, type: :fixed64
end

defmodule Sentinel.Model.V1.SourceIdentity do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.SourceIdentity",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :monitored_application_id, 1, type: :string, json_name: "monitoredApplicationId"
  field :source_id, 2, type: :string, json_name: "sourceId"
end

defmodule Sentinel.Model.V1.SequenceCoordinate do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.SequenceCoordinate",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :epoch, 1, type: :uint64
  field :sequence, 2, type: :uint64
end

defmodule Sentinel.Model.V1.OccurrenceTime do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.OccurrenceTime",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :clock_domain_id, 1, type: :string, json_name: "clockDomainId"
  field :nanoseconds, 2, type: Sentinel.Model.V1.Int128
  field :uncertainty_nanoseconds, 3, type: :uint64, json_name: "uncertaintyNanoseconds"
end

defmodule Sentinel.Model.V1.ObservationTime do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.ObservationTime",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :sentinel_clock_id, 1, type: :string, json_name: "sentinelClockId"
  field :nanoseconds, 2, type: Sentinel.Model.V1.Int128
end

defmodule Sentinel.Model.V1.SourceAssurance do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.SourceAssurance",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :source, 1, type: Sentinel.Model.V1.SourceIdentity
  field :tier, 2, type: Sentinel.Model.V1.SourceTier, enum: true

  field :effective_capabilities, 3,
    repeated: true,
    type: Sentinel.Model.V1.SourceCapability,
    json_name: "effectiveCapabilities",
    enum: true

  field :integrity, 4, type: Sentinel.Model.V1.Integrity, enum: true

  field :authenticated_identity_evidence, 5,
    type: :bytes,
    json_name: "authenticatedIdentityEvidence"

  field :integrity_evidence, 6, type: :bytes, json_name: "integrityEvidence"
  field :topology_id, 7, type: :string, json_name: "topologyId"
end

defmodule Sentinel.Model.V1.ProducerCompletenessSignal do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.ProducerCompletenessSignal",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :sequence_epoch, 1, type: :uint64, json_name: "sequenceEpoch"
  field :last_emitted, 2, proto3_optional: true, type: :uint64, json_name: "lastEmitted"
  field :complete_through, 3, proto3_optional: true, type: :uint64, json_name: "completeThrough"

  field :producer_reported_at, 4,
    type: Sentinel.Model.V1.OccurrenceTime,
    json_name: "producerReportedAt"

  field :acknowledged_filter_epoch, 5,
    proto3_optional: true,
    type: :uint64,
    json_name: "acknowledgedFilterEpoch"

  field :activity, 6, type: Sentinel.Model.V1.SourceActivityState, enum: true
  field :activity_since, 7, type: Sentinel.Model.V1.OccurrenceTime, json_name: "activitySince"
end

defmodule Sentinel.Model.V1.SourceCompletenessSignal do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.SourceCompletenessSignal",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :producer_signal, 1,
    type: Sentinel.Model.V1.ProducerCompletenessSignal,
    json_name: "producerSignal"

  field :assurance, 2, type: Sentinel.Model.V1.SourceAssurance
  field :observed_at, 3, type: Sentinel.Model.V1.ObservationTime, json_name: "observedAt"
end

defmodule Sentinel.Model.V1.Watermark do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.Watermark",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :source, 1, type: Sentinel.Model.V1.SourceIdentity
  field :sequence_epoch, 2, type: :uint64, json_name: "sequenceEpoch"
  field :complete_through, 3, type: :uint64, json_name: "completeThrough"
  field :observed_at, 4, type: Sentinel.Model.V1.ObservationTime, json_name: "observedAt"
end
