defmodule Sentinel.Probe.SDK.Enforcement.Budget do
  @moduledoc """
  Monotonic budget arithmetic. Elixir analog of `sdk/go/enforcement/budget.go`
  (`monotonic-budget.ts`).

  `deadline_ns` is a monotonic ABSOLUTE, computed by the caller at gate entry as
  `now_monotonic_ns + latency_budget_ns`. `now_ns` is the current monotonic
  reading, injected. A passed deadline yields `0`, never a negative budget.

  Pure: no clock access, no side effects. The clock is injected through
  `Deps.now_monotonic_ns/0` and never read inside, so budget behaviour is
  testable without sleeping.
  """

  @doc """
  Computes the remaining transport budget from a monotonic absolute deadline.

  Returns `0` when the deadline has passed (never negative).
  """
  @spec remaining_transport_budget_ns(deadline_ns :: integer(), now_ns :: integer()) ::
          non_neg_integer()
  def remaining_transport_budget_ns(deadline_ns, now_ns) do
    remaining = deadline_ns - now_ns

    if remaining <= 0, do: 0, else: remaining
  end
end
