defmodule Sentinel.Probe.SDK.Internal.SpecMatch do
  @moduledoc """
  The single definition of "does this `SpecificationFilter` select this
  `ProducerEvent`", shared by the Filter and Enforcement packages.

  Elixir analog of `sdk/go/internal/specmatch/specmatch.go`. It lives under
  `Internal.` deliberately: in the TypeScript reference `specSelects` is exported
  from `apply-filter.ts` so the enforcement gate can reuse it (a review fix, to
  stop the gate carrying a second, drifting copy of the predicate), but it is NOT
  re-exported from the package barrel, so it is not public SDK API. `Internal.`
  reproduces that visibility without widening the surface.
  """

  alias Sentinel.Model.V1.{ProducerEvent, SpecificationFilter}

  @doc """
  Reports whether `spec`'s `EventMatch` selects `event`: its `event_kinds` is
  empty, or it includes `event.kind`.

  A spec with no `EventMatch` at all selects everything, defensively — the same
  over-approximate-upward choice ADR-0006 requires of the projection itself.
  `event_match` is a sub-message that defaults to a struct with empty lists when
  unset, so `nil`-safety is structural rather than optional-chaining.
  """
  @spec selects?(SpecificationFilter.t() | nil, ProducerEvent.t() | nil) :: boolean()
  def selects?(nil, _event), do: true

  def selects?(%SpecificationFilter{event_match: nil}, _event), do: true

  def selects?(%SpecificationFilter{event_match: %{event_kinds: []}}, _event), do: true

  def selects?(%SpecificationFilter{event_match: %{event_kinds: kinds}}, %ProducerEvent{kind: kind}) do
    kind in kinds
  end
end
