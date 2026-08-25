defmodule Sentinel.Probe.SDK.BudgetTest do
  use ExUnit.Case, async: true

  alias Sentinel.Probe.SDK.Enforcement.Budget

  @cases [
    {"budget remains", 10000, 5000, 5000},
    {"exactly exhausted", 10000, 10000, 0},
    {"deadline passed", 10000, 99999, 0},
    {"full budget", 10000, 0, 10000},
    {"negative clock origin", 0, -5000, 5000},
    {"signed boundary wrap", -9_223_372_036_854_775_804, 9_223_372_036_854_775_802, 10},
    {"ambiguous half range", -9_223_372_036_854_775_808, 0, 0},
    {"arbitrary width low-word normalization", 18_446_744_073_709_551_621,
     18_446_744_073_709_551_611, 10}
  ]

  test "remaining_transport_budget_ns computes the relative budget from an absolute deadline" do
    for {name, deadline, now, want} <- @cases do
      assert Budget.remaining_transport_budget_ns(deadline, now) == want, "#{name}"
    end
  end
end
