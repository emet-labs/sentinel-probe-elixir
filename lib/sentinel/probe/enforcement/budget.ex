defmodule Sentinel.Probe.SDK.Enforcement.Budget do
  import Bitwise

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
    monotonic_delta_ns(now_ns, deadline_ns) || 0
  end

  @modulus 1 <<< 64
  @half 1 <<< 63

  def monotonic_delta_ns(from, to) do
    delta = Integer.mod(Integer.mod(to, @modulus) - Integer.mod(from, @modulus), @modulus)
    if delta < @half, do: delta, else: nil
  end

  def remaining_budget_ns(anchor, budget, now) do
    case monotonic_delta_ns(anchor, now) do
      elapsed when is_integer(elapsed) and elapsed < budget -> budget - elapsed
      _ -> 0
    end
  end
end
