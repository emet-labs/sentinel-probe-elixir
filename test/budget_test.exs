defmodule Sentinel.Probe.SDK.BudgetTest do
  use ExUnit.Case, async: true

  alias Sentinel.Probe.SDK.Enforcement.Budget

  @cases [
    {"budget remains", 10000, 5000, 5000},
    {"exactly exhausted", 10000, 10000, 0},
    {"deadline passed", 10000, 99999, 0},
    {"full budget", 10000, 0, 10000},
    {"negative clock origin", 0, -5000, 5000}
  ]

  test "remaining_transport_budget_ns computes the relative budget from an absolute deadline" do
    for {name, deadline, now, want} <- @cases do
      assert Budget.remaining_transport_budget_ns(deadline, now) == want, "#{name}"
    end
  end
end
