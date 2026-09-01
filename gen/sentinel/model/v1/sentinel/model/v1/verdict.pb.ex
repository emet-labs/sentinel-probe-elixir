defmodule Sentinel.Model.V1.VerdictOutcome do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "sentinel.model.v1.VerdictOutcome",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :VERDICT_OUTCOME_UNSPECIFIED, 0
  field :VERDICT_OUTCOME_SATISFY, 1
  field :VERDICT_OUTCOME_VIOLATE, 2
  field :VERDICT_OUTCOME_INCONCLUSIVE, 3
end

defmodule Sentinel.Model.V1.InconclusiveReason do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "sentinel.model.v1.InconclusiveReason",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :INCONCLUSIVE_REASON_UNSPECIFIED, 0
  field :INCONCLUSIVE_REASON_PREFIX_UNCERTAINTY, 1
  field :INCONCLUSIVE_REASON_OBSERVATIONAL_UNCERTAINTY, 2
  field :INCONCLUSIVE_REASON_EPISTEMIC_UNCERTAINTY, 3
end

defmodule Sentinel.Model.V1.VerdictAuthority do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "sentinel.model.v1.VerdictAuthority",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :VERDICT_AUTHORITY_UNSPECIFIED, 0
  field :VERDICT_AUTHORITY_PROVISIONAL, 1
  field :VERDICT_AUTHORITY_AUTHORITATIVE, 2
end

defmodule Sentinel.Model.V1.TierSettlement do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "sentinel.model.v1.TierSettlement",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :TIER_SETTLEMENT_UNSPECIFIED, 0
  field :TIER_SETTLEMENT_NOT_REQUIRED, 1
  field :TIER_SETTLEMENT_PENDING, 2
  field :TIER_SETTLEMENT_SETTLED, 3
end

defmodule Sentinel.Model.V1.LateDataPolicy do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "sentinel.model.v1.LateDataPolicy",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :LATE_DATA_POLICY_UNSPECIFIED, 0
  field :LATE_DATA_POLICY_EXCLUDE, 1
  field :LATE_DATA_POLICY_ACCEPT_FOR_FUTURE_VERDICTS, 2
end

defmodule Sentinel.Model.V1.FilterEpochBySource do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.FilterEpochBySource",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :source, 1, type: Sentinel.Model.V1.SourceIdentity
  field :filter_epoch, 2, proto3_optional: true, type: :uint64, json_name: "filterEpoch"
end

defmodule Sentinel.Model.V1.SourceGap do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.SourceGap",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :source, 1, type: Sentinel.Model.V1.SourceIdentity
  field :sequence_epoch, 2, type: :uint64, json_name: "sequenceEpoch"
  field :first_missing, 3, type: :uint64, json_name: "firstMissing"
  field :last_missing, 4, type: :uint64, json_name: "lastMissing"
  field :effect, 5, type: :string
end

defmodule Sentinel.Model.V1.SourceHealth do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.SourceHealth",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :source, 1, type: Sentinel.Model.V1.SourceIdentity
  field :healthy, 2, type: :bool
  field :detail, 3, type: :string
end

defmodule Sentinel.Model.V1.SourceCapabilityRecord do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.SourceCapabilityRecord",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :source, 1, type: Sentinel.Model.V1.SourceIdentity
  field :capabilities, 2, repeated: true, type: Sentinel.Model.V1.SourceCapability, enum: true
end

defmodule Sentinel.Model.V1.SourceTierRecord do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.SourceTierRecord",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :source, 1, type: Sentinel.Model.V1.SourceIdentity
  field :tier, 2, type: Sentinel.Model.V1.SourceTier, enum: true
  field :anchored_event_ids, 3, repeated: true, type: :string, json_name: "anchoredEventIds"
end

defmodule Sentinel.Model.V1.UnresolvedCausalEdge do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.UnresolvedCausalEdge",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :event_id, 1, type: :string, json_name: "eventId"
  field :missing_predecessor_id, 2, type: :string, json_name: "missingPredecessorId"
end

defmodule Sentinel.Model.V1.ExtractionFact do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.ExtractionFact",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :fact_id, 1, type: :string, json_name: "factId"
  field :available, 2, type: :bool
  field :detail, 3, type: :string
end

defmodule Sentinel.Model.V1.Uncertainty do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.Uncertainty",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :reason, 1, type: Sentinel.Model.V1.InconclusiveReason, enum: true
  field :detail, 2, type: :string
end

defmodule Sentinel.Model.V1.SourceTierGap do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.SourceTierGap",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :contributing_sources, 1,
    repeated: true,
    type: Sentinel.Model.V1.SourceIdentity,
    json_name: "contributingSources"

  field :detail, 2, type: :string

  field :unanchored_sources, 3,
    repeated: true,
    type: Sentinel.Model.V1.SourceIdentity,
    json_name: "unanchoredSources"
end

defmodule Sentinel.Model.V1.DecisiveEvent do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.DecisiveEvent",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :event_id, 1, type: :string, json_name: "eventId"
  field :source, 2, type: Sentinel.Model.V1.SourceIdentity
  field :sequence_epoch, 3, proto3_optional: true, type: :uint64, json_name: "sequenceEpoch"
  field :sequence_number, 4, proto3_optional: true, type: :uint64, json_name: "sequenceNumber"
end

defmodule Sentinel.Model.V1.OutcomeInvariantLimitations do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.OutcomeInvariantLimitations",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :gaps, 1, repeated: true, type: Sentinel.Model.V1.SourceGap

  field :unhealthy_sources, 2,
    repeated: true,
    type: Sentinel.Model.V1.SourceIdentity,
    json_name: "unhealthySources"

  field :unresolved_causal_edges, 3,
    repeated: true,
    type: Sentinel.Model.V1.UnresolvedCausalEdge,
    json_name: "unresolvedCausalEdges"

  field :unavailable_extraction_fact_ids, 4,
    repeated: true,
    type: :string,
    json_name: "unavailableExtractionFactIds"

  field :uncertainty, 5, repeated: true, type: Sentinel.Model.V1.InconclusiveReason, enum: true
end

defmodule Sentinel.Model.V1.DecisiveViolationBasis do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.DecisiveViolationBasis",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :witness_events, 1,
    repeated: true,
    type: Sentinel.Model.V1.DecisiveEvent,
    json_name: "witnessEvents"

  field :required_extraction_fact_ids, 2,
    repeated: true,
    type: :string,
    json_name: "requiredExtractionFactIds"

  field :invariance_proof_rule_id, 3, type: :string, json_name: "invarianceProofRuleId"
  field :invariance_proof_rule_version, 4, type: :string, json_name: "invarianceProofRuleVersion"

  field :outcome_invariant_limitations, 5,
    type: Sentinel.Model.V1.OutcomeInvariantLimitations,
    json_name: "outcomeInvariantLimitations"
end

defmodule Sentinel.Model.V1.CompleteEvidenceBasis do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.CompleteEvidenceBasis",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3
end

defmodule Sentinel.Model.V1.SettlementBasis do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.SettlementBasis",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  oneof :basis, 0

  field :complete_evidence, 1,
    type: Sentinel.Model.V1.CompleteEvidenceBasis,
    json_name: "completeEvidence",
    oneof: 0

  field :decisive_violation, 2,
    type: Sentinel.Model.V1.DecisiveViolationBasis,
    json_name: "decisiveViolation",
    oneof: 0
end

defmodule Sentinel.Model.V1.AssuranceEnvelope do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.AssuranceEnvelope",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :specification_id, 1, type: :string, json_name: "specificationId"
  field :specification_version, 2, type: :string, json_name: "specificationVersion"
  field :schema_versions, 3, repeated: true, type: :string, json_name: "schemaVersions"

  field :filter_epochs, 4,
    repeated: true,
    type: Sentinel.Model.V1.FilterEpochBySource,
    json_name: "filterEpochs"

  field :watermarks, 5, repeated: true, type: Sentinel.Model.V1.Watermark

  field :required_sources, 6,
    repeated: true,
    type: Sentinel.Model.V1.SourceIdentity,
    json_name: "requiredSources"

  field :observed_sources, 7,
    repeated: true,
    type: Sentinel.Model.V1.SourceIdentity,
    json_name: "observedSources"

  field :gaps, 8, repeated: true, type: Sentinel.Model.V1.SourceGap

  field :source_health, 9,
    repeated: true,
    type: Sentinel.Model.V1.SourceHealth,
    json_name: "sourceHealth"

  field :capabilities, 10, repeated: true, type: Sentinel.Model.V1.SourceCapabilityRecord

  field :source_tiers_and_anchors, 11,
    repeated: true,
    type: Sentinel.Model.V1.SourceTierRecord,
    json_name: "sourceTiersAndAnchors"

  field :unresolved_causal_edges, 12,
    repeated: true,
    type: Sentinel.Model.V1.UnresolvedCausalEdge,
    json_name: "unresolvedCausalEdges"

  field :extraction_facts, 13,
    repeated: true,
    type: Sentinel.Model.V1.ExtractionFact,
    json_name: "extractionFacts"

  field :uncertainty, 14, repeated: true, type: Sentinel.Model.V1.Uncertainty

  field :late_data_policy, 15,
    type: Sentinel.Model.V1.LateDataPolicy,
    json_name: "lateDataPolicy",
    enum: true

  field :hot_path, 16, type: Sentinel.Model.V1.TierSettlement, json_name: "hotPath", enum: true
  field :cold_path, 17, type: Sentinel.Model.V1.TierSettlement, json_name: "coldPath", enum: true

  field :hot_settlement_watermarks, 18,
    repeated: true,
    type: Sentinel.Model.V1.Watermark,
    json_name: "hotSettlementWatermarks"

  field :cold_settlement_watermarks, 19,
    repeated: true,
    type: Sentinel.Model.V1.Watermark,
    json_name: "coldSettlementWatermarks"

  field :source_tier_gap, 20, type: Sentinel.Model.V1.SourceTierGap, json_name: "sourceTierGap"

  field :settlement_basis, 21,
    type: Sentinel.Model.V1.SettlementBasis,
    json_name: "settlementBasis"
end

defmodule Sentinel.Model.V1.Verdict do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.Verdict",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :id, 1, type: :string
  field :outcome, 2, type: Sentinel.Model.V1.VerdictOutcome, enum: true

  field :inconclusive_reason, 3,
    proto3_optional: true,
    type: Sentinel.Model.V1.InconclusiveReason,
    json_name: "inconclusiveReason",
    enum: true

  field :authority, 4, type: Sentinel.Model.V1.VerdictAuthority, enum: true
  field :assurance, 5, type: Sentinel.Model.V1.AssuranceEnvelope
end
