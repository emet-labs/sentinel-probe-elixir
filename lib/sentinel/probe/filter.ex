defmodule Sentinel.Probe.SDK.Filter do
  @moduledoc """
  Attribute-level Event Filter projection (ADR-0006). Elixir analog of
  `sdk/typescript/src/filter/apply-filter.ts` and `sdk/go/filter/apply.go`.

  This is RELEVANCE PROJECTION, not sampling: it never drops a relevant event,
  never drops an attribute any selecting Specification could need, and never
  invents data. Where the answer is uncertain it over-approximates upward —
  keeping more than strictly necessary is sound; keeping less is not.
  """

  alias Sentinel.Model.V1.{AttributeEntry, EventFilter, ProducerEvent}
  alias Sentinel.Probe.SDK.Internal.SpecMatch

  @doc """
  Projects `event` against `filter`.

  Returns a possibly attribute-trimmed `ProducerEvent` when at least one
  `SpecificationFilter`'s `EventMatch` selects the event, and `nil` when none
  does — meaning the event is irrelevant to every Specification and can be
  dropped entirely.

  The algorithm mirrors the reference step for step:

    1. collect the `SpecificationFilter`s whose `EventMatch` selects the event;
    2. none selecting means drop, sound because no Specification depends on it;
    3. if ANY selecting spec has an empty `projected_attribute_keys`, keep every
       attribute (over-approximate upward: that spec might need any of them);
    4. otherwise keep the union of the selecting specs' projected keys;
    5. rebuild the event with every other field unchanged. `causal_predecessor_ids`
       is NEVER trimmed — it is the causal skeleton, not an attribute.

  A `nil` filter behaves as a filter with no specifications, i.e. drop.

  ## Aliasing

  The returned event always carries a FRESHLY ALLOCATED attribute list, including
  in the keep-everything branch. The `AttributeEntry` structs themselves are
  shared with the input, so a caller must not mutate an entry in place — proto
  messages are immutable by convention and no deep copy is paid for on the emit
  hot path, exactly as in the Go analog.
  """
  @spec apply_filter(ProducerEvent.t() | nil, EventFilter.t() | nil) :: ProducerEvent.t() | nil
  def apply_filter(nil, _filter), do: nil

  def apply_filter(%ProducerEvent{} = _event, nil), do: nil

  def apply_filter(%ProducerEvent{} = event, %EventFilter{} = filter) do
    # 1. Collect the SpecificationFilters whose EventMatch selects the event.
    selecting = Enum.filter(filter.specifications, &SpecMatch.selects?(&1, event))

    # 2. No spec selects: drop entirely.
    if selecting == [] do
      nil
    else
      trimmed = trim_attributes(event.attributes, selecting)

      # 5. Rebuild with every other field unchanged.
      %ProducerEvent{
        id: event.id,
        sequence: event.sequence,
        schema_version: event.schema_version,
        acknowledged_filter_epoch: event.acknowledged_filter_epoch,
        kind: event.kind,
        occurrence_time: event.occurrence_time,
        attributes: trimmed,
        claimed_capabilities: event.claimed_capabilities,
        claimed_sensitivity: event.claimed_sensitivity,
        causal_predecessor_ids: event.causal_predecessor_ids
      }
    end
  end

  # 3 & 4. If any selecting spec has empty projected_attribute_keys, keep every
  # attribute; otherwise keep the union of the selecting specs' projected keys.
  defp trim_attributes(attributes, selecting) do
    if Enum.any?(selecting, &project_all?/1) do
      # Fresh list allocation (copy the references), matching the Go analog.
      Enum.to_list(attributes)
    else
      projected =
        selecting
        |> Enum.flat_map(fn spec -> spec.event_match.projected_attribute_keys end)
        |> MapSet.new()

      Enum.filter(attributes, fn %AttributeEntry{key: key} -> MapSet.member?(projected, key) end)
    end
  end

  defp project_all?(%{event_match: nil}), do: true
  defp project_all?(%{event_match: %{projected_attribute_keys: []}}), do: true
  defp project_all?(_), do: false
end
