defmodule Sentinel.Model.V1.AttributeArray do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.AttributeArray",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :values, 1, repeated: true, type: Sentinel.Model.V1.AttributeValue
end

defmodule Sentinel.Model.V1.AttributeMap do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.AttributeMap",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :entries, 1, repeated: true, type: Sentinel.Model.V1.AttributeEntry
end

defmodule Sentinel.Model.V1.AttributeEntry do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.AttributeEntry",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :key, 1, type: :string
  field :value, 2, type: Sentinel.Model.V1.AttributeValue

  field :claimed_sensitivity, 3,
    type: Sentinel.Model.V1.Sensitivity,
    json_name: "claimedSensitivity",
    enum: true
end

defmodule Sentinel.Model.V1.AttributeValue do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.AttributeValue",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  oneof :value, 0

  field :string_value, 1, type: :string, json_name: "stringValue", oneof: 0
  field :bool_value, 2, type: :bool, json_name: "boolValue", oneof: 0
  field :integer_value, 3, type: :sint64, json_name: "integerValue", oneof: 0
  field :double_value, 4, type: :double, json_name: "doubleValue", oneof: 0
  field :bytes_value, 5, type: :bytes, json_name: "bytesValue", oneof: 0
  field :array_value, 6, type: Sentinel.Model.V1.AttributeArray, json_name: "arrayValue", oneof: 0
  field :map_value, 7, type: Sentinel.Model.V1.AttributeMap, json_name: "mapValue", oneof: 0
end

defmodule Sentinel.Model.V1.ProducerEvent do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.ProducerEvent",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :id, 1, type: :string
  field :sequence, 2, type: Sentinel.Model.V1.SequenceCoordinate
  field :schema_version, 3, type: :string, json_name: "schemaVersion"

  field :acknowledged_filter_epoch, 4,
    proto3_optional: true,
    type: :uint64,
    json_name: "acknowledgedFilterEpoch"

  field :kind, 5, type: :string
  field :occurrence_time, 6, type: Sentinel.Model.V1.OccurrenceTime, json_name: "occurrenceTime"
  field :attributes, 7, repeated: true, type: Sentinel.Model.V1.AttributeEntry

  field :claimed_capabilities, 8,
    repeated: true,
    type: Sentinel.Model.V1.SourceCapability,
    json_name: "claimedCapabilities",
    enum: true

  field :claimed_sensitivity, 9,
    type: Sentinel.Model.V1.Sensitivity,
    json_name: "claimedSensitivity",
    enum: true

  field :causal_predecessor_ids, 10,
    repeated: true,
    type: :string,
    json_name: "causalPredecessorIds"
end

defmodule Sentinel.Model.V1.Event do
  @moduledoc false

  use Protobuf,
    full_name: "sentinel.model.v1.Event",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :producer_event, 1, type: Sentinel.Model.V1.ProducerEvent, json_name: "producerEvent"
  field :assurance, 2, type: Sentinel.Model.V1.SourceAssurance

  field :observation_time, 3,
    type: Sentinel.Model.V1.ObservationTime,
    json_name: "observationTime"

  field :tenant_id, 4, type: :string, json_name: "tenantId"
  field :deployment_id, 5, type: :string, json_name: "deploymentId"
  field :data_class, 6, type: Sentinel.Model.V1.DataClass, json_name: "dataClass", enum: true
end
