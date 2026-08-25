defmodule Sentinel.Probe.SDK.GateTest do
  use ExUnit.Case, async: true

  import Sentinel.Probe.SDK.TestHelpers

  alias Sentinel.Probe.SDK.Client.Transport
  alias Sentinel.Probe.SDK.Enforcement.Gate
  alias Sentinel.Probe.SDK.MockDecider

  setup do
    MockDecider.reset!()
    :ok
  end

  defp gate(event, filter, deadline, deps) do
    Gate.gate(event, filter, deadline, deps, test_options())
  end

  defp deps_with_clock(mock, clock) do
    %Gate.Deps{
      decide: fn req -> MockDecider.decide(mock, req) end,
      now_monotonic_ns: clock,
      accepted_fail_mode_for: fn _ -> :FAIL_MODE_OPEN end
    }
  end

  test "permit when the decision endpoint permits" do
    mock = MockDecider.new(response: make_response(:DECISION_ACTION_PERMIT))

    outcome =
      gate(
        make_event(test_kind()),
        make_filter(test_epoch(), [ask_and_block_spec()]),
        10000,
        make_deps(mock, 0, nil)
      )

    assert outcome.kind == :permit
    assert outcome.filter_epoch == test_epoch()
    assert Gate.permitted?(outcome.kind)
  end

  test "deny when the decision endpoint denies" do
    mock = MockDecider.new(response: make_response(:DECISION_ACTION_DENY))

    outcome =
      gate(
        make_event(test_kind()),
        make_filter(test_epoch(), [ask_and_block_spec()]),
        10000,
        make_deps(mock, 0, nil)
      )

    assert outcome.kind == :deny
    refute Gate.permitted?(outcome.kind)
  end

  test "defer while budget remains" do
    mock = MockDecider.new(response: make_response(:DECISION_ACTION_DEFER))

    outcome =
      gate(
        make_event(test_kind()),
        make_filter(test_epoch(), [ask_and_block_spec()]),
        10000,
        make_deps(mock, 0, nil)
      )

    assert outcome.kind == :defer
  end

  test "defer-budget-exhausted fails open when the clock advances past the deadline" do
    mock = MockDecider.new(response: make_response(:DECISION_ACTION_DEFER))
    # The clock advances between entry and response: at entry budget remains, so
    # the ask happens; after the response budget is gone.
    Process.put(:clock_calls, 0)

    clock = fn ->
      n = Process.get(:clock_calls, 0) + 1
      Process.put(:clock_calls, n)
      if n <= 2, do: 0, else: 10000
    end

    deps = %Gate.Deps{
      decide: fn req -> MockDecider.decide(mock, req) end,
      now_monotonic_ns: clock,
      accepted_fail_mode_for: fn _ -> :FAIL_MODE_OPEN end
    }

    outcome =
      gate(make_event(test_kind()), make_filter(test_epoch(), [ask_and_block_spec()]), 10000, deps)

    assert outcome.kind == :fail_open_permit
    assert outcome.reason == "defer-budget-exhausted"
    assert MockDecider.call_count() == 1
  end

  test "defer-budget-exhausted fails closed when contracted" do
    mock = MockDecider.new(response: make_response(:DECISION_ACTION_DEFER))
    Process.put(:clock_calls, 0)

    clock = fn ->
      n = Process.get(:clock_calls, 0) + 1
      Process.put(:clock_calls, n)
      if n <= 2, do: 0, else: 10000
    end

    deps = %Gate.Deps{
      decide: fn req -> MockDecider.decide(mock, req) end,
      now_monotonic_ns: clock,
      accepted_fail_mode_for: &always_closed/1
    }

    outcome =
      gate(
        make_event(test_kind()),
        make_filter(test_epoch(), [closed_ask_and_block_spec()]),
        10000,
        deps
      )

    assert outcome.kind == :fail_closed_deny
  end

  test "transport error fails open by default" do
    mock = MockDecider.new(error: "connection refused")

    outcome =
      gate(
        make_event(test_kind()),
        make_filter(test_epoch(), [ask_and_block_spec()]),
        10000,
        make_deps(mock, 0, nil)
      )

    assert outcome.kind == :fail_open_permit
    assert outcome.reason == "transport-error: connection refused"
  end

  test "transport error fails closed when contracted" do
    mock = MockDecider.new(error: "connection refused")

    outcome =
      gate(
        make_event(test_kind()),
        make_filter(test_epoch(), [closed_ask_and_block_spec()]),
        10000,
        make_deps(mock, 0, &always_closed/1)
      )

    assert outcome.kind == :fail_closed_deny
  end

  test "declaring CLOSED is not enough: an accepted contract is required" do
    mock = MockDecider.new(error: "connection refused")

    outcome =
      gate(
        make_event(test_kind()),
        make_filter(test_epoch(), [closed_ask_and_block_spec()]),
        10000,
        make_deps(mock, 0, nil)
      )

    assert outcome.kind == :fail_open_permit
  end

  test "a nil AcceptedFailModeFor raises, not defaults to fail-open" do
    mock = MockDecider.new(error: "connection refused")

    deps = %Gate.Deps{
      decide: fn req -> MockDecider.decide(mock, req) end,
      now_monotonic_ns: fn -> 0 end,
      accepted_fail_mode_for: nil
    }

    assert_raise RuntimeError, ~r/Deps.AcceptedFailModeFor is required/, fn ->
      gate(
        make_event(test_kind()),
        make_filter(test_epoch(), [closed_ask_and_block_spec()]),
        10000,
        deps
      )
    end
  end

  test "the aggregate fail mode: one contracted-closed spec among many open wins" do
    open = make_spec("open", [test_kind()], :FAIL_MODE_OPEN, :DELIVERY_MODE_ASK_AND_BLOCK)
    closed = make_spec("closed", [test_kind()], :FAIL_MODE_CLOSED, :DELIVERY_MODE_ASK_AND_BLOCK)

    mock = MockDecider.new(error: "boom")

    outcome =
      gate(
        make_event(test_kind()),
        make_filter(test_epoch(), [open, closed]),
        10000,
        make_deps(mock, 0, &always_closed/1)
      )

    assert outcome.kind == :fail_closed_deny
  end

  test "an unspecified fail mode is open" do
    spec = make_spec("s", [test_kind()], :FAIL_MODE_UNSPECIFIED, :DELIVERY_MODE_ASK_AND_BLOCK)
    mock = MockDecider.new(error: "boom")

    outcome =
      gate(
        make_event(test_kind()),
        make_filter(test_epoch(), [spec]),
        10000,
        make_deps(mock, 0, nil)
      )

    assert outcome.kind == :fail_open_permit
  end

  test "ship-async specs are permitted without asking" do
    mock = MockDecider.new(response: make_response(:DECISION_ACTION_DENY))

    outcome =
      gate(
        make_event(test_kind()),
        make_filter(test_epoch(), [ship_async_spec()]),
        10000,
        make_deps(mock, 0, nil)
      )

    assert outcome.kind == :permit
    assert outcome.reason == "no-ask-and-block-spec"
    assert MockDecider.call_count() == 0
  end

  test "a non-selecting spec is not enforcing" do
    other = make_spec("s", ["other.kind"], :FAIL_MODE_CLOSED, :DELIVERY_MODE_ASK_AND_BLOCK)
    mock = MockDecider.new(response: make_response(:DECISION_ACTION_DENY))

    outcome =
      gate(
        make_event(test_kind()),
        make_filter(test_epoch(), [other]),
        10000,
        make_deps(mock, 0, &always_closed/1)
      )

    assert outcome.kind == :permit
    assert MockDecider.call_count() == 0
  end

  test "budget exhausted before the call skips Decide" do
    mock = MockDecider.new(response: make_response(:DECISION_ACTION_PERMIT))

    outcome =
      gate(
        make_event(test_kind()),
        make_filter(test_epoch(), [ask_and_block_spec()]),
        10000,
        make_deps(mock, 99999, nil)
      )

    assert outcome.kind == :fail_open_permit
    assert outcome.reason == "budget-exhausted"
    assert MockDecider.call_count() == 0
  end

  test "a deadline already past skips Decide" do
    mock = MockDecider.new(response: make_response(:DECISION_ACTION_PERMIT))

    outcome =
      gate(
        make_event(test_kind()),
        make_filter(test_epoch(), [ask_and_block_spec()]),
        0,
        make_deps(mock, 5000, nil)
      )

    assert outcome.kind == :fail_open_permit
    assert MockDecider.call_count() == 0
  end

  test "an unspecified action is never a blind permit" do
    mock = MockDecider.new(response: make_response(:DECISION_ACTION_UNSPECIFIED))

    outcome =
      gate(
        make_event(test_kind()),
        make_filter(test_epoch(), [ask_and_block_spec()]),
        10000,
        make_deps(mock, 0, nil)
      )

    assert outcome.kind == :fail_open_permit
    assert outcome.reason == "unspecified-action"
  end

  test "an unspecified action fails closed when contracted" do
    mock = MockDecider.new(response: make_response(:DECISION_ACTION_UNSPECIFIED))

    outcome =
      gate(
        make_event(test_kind()),
        make_filter(test_epoch(), [closed_ask_and_block_spec()]),
        10000,
        make_deps(mock, 0, &always_closed/1)
      )

    assert outcome.kind == :fail_closed_deny
  end

  test "the wire budget comes from the injected clock" do
    mock = MockDecider.new(response: make_response(:DECISION_ACTION_PERMIT))

    gate(
      make_event(test_kind()),
      make_filter(test_epoch(), [ask_and_block_spec()]),
      10000,
      make_deps(mock, 3000, nil)
    )

    req = MockDecider.last_request()
    assert req.remaining_transport_budget_nanoseconds == 7000
  end

  test "without a caller budget, the Specification budget is sent" do
    mock = MockDecider.new(response: make_response(:DECISION_ACTION_PERMIT))

    outcome =
      gate(
        make_event(test_kind()),
        make_filter(test_epoch(), [ask_and_block_spec()]),
        nil,
        make_deps(mock, 0, nil)
      )

    assert outcome.kind == :permit
    assert MockDecider.last_request().remaining_transport_budget_nanoseconds == 10_000
  end

  test "without a caller budget, defer uses the Specification budget" do
    mock = MockDecider.new(response: make_response(:DECISION_ACTION_DEFER))

    outcome =
      gate(
        make_event(test_kind()),
        make_filter(test_epoch(), [ask_and_block_spec()]),
        nil,
        make_deps(mock, 0, nil)
      )

    assert outcome.kind == :defer
  end

  test "without a budget, a transport error fails open" do
    mock = MockDecider.new(error: "boom")

    outcome =
      gate(
        make_event(test_kind()),
        make_filter(test_epoch(), [ask_and_block_spec()]),
        nil,
        make_deps(mock, 0, nil)
      )

    assert outcome.kind == :fail_open_permit
  end

  test "no filter held returns no-filter" do
    mock = MockDecider.new(response: make_response(:DECISION_ACTION_PERMIT))
    outcome = gate(make_event(test_kind()), nil, 10000, make_deps(mock, 0, nil))

    assert outcome.kind == :no_filter
    assert outcome.reason == "no-filter"
    assert Gate.permitted?(:no_filter)
    assert MockDecider.call_count() == 0
  end

  test "a filter without an epoch is treated as no-filter" do
    mock = MockDecider.new(response: make_response(:DECISION_ACTION_PERMIT))

    outcome =
      gate(
        make_event(test_kind()),
        make_filter(nil, [ask_and_block_spec()]),
        10000,
        make_deps(mock, 0, nil)
      )

    assert outcome.kind == :no_filter
  end

  test "epoch 0 is a legitimate filter, not no-filter" do
    mock = MockDecider.new(response: make_response(:DECISION_ACTION_PERMIT))

    outcome =
      gate(
        make_event(test_kind()),
        make_filter(0, [ask_and_block_spec()]),
        10000,
        make_deps(mock, 0, nil)
      )

    assert outcome.kind == :permit
    assert outcome.filter_epoch == 0
  end

  test "the request carries identifiers, source handle and the event" do
    mock = MockDecider.new(response: make_response(:DECISION_ACTION_PERMIT))

    gate(
      make_event(test_kind()),
      make_filter(test_epoch(), [ask_and_block_spec()]),
      10000,
      make_deps(mock, 0, nil)
    )

    req = MockDecider.last_request()
    assert req.request_id == "req-1"
    assert req.idempotency_key == "idem-1"
    assert req.source_handle == "gateway.tool-calls"
    assert req.filter_epoch == test_epoch()
    assert req.producer_event.kind == test_kind()
  end

  test "every outcome audits the filter epoch and carries a reason" do
    epoch = 42

    cases = [
      {"permit", MockDecider.new(response: make_response(:DECISION_ACTION_PERMIT))},
      {"deny", MockDecider.new(response: make_response(:DECISION_ACTION_DENY))},
      {"defer", MockDecider.new(response: make_response(:DECISION_ACTION_DEFER))},
      {"unspecified", MockDecider.new(response: make_response(:DECISION_ACTION_UNSPECIFIED))},
      {"transport error", MockDecider.new(error: "boom")}
    ]

    for {name, mock} <- cases do
      MockDecider.reset!()

      outcome =
        gate(
          make_event(test_kind()),
          make_filter(epoch, [ask_and_block_spec()]),
          10000,
          make_deps(mock, 0, nil)
        )

      assert outcome.filter_epoch == epoch, "#{name}: filter epoch"
      assert outcome.reason != "", "#{name}: reason must be populated (D15)"
    end
  end

  test "per-spec decisions are surfaced on the outcome (D11)" do
    reason = :UNRESOLVED_REASON_EVIDENCE_GAP

    mock =
      MockDecider.new(
        response:
          make_response(:DECISION_ACTION_DEFER, [
            make_decision("spec-1", :DECISION_ACTION_DEFER, reason)
          ])
      )

    outcome =
      gate(
        make_event(test_kind()),
        make_filter(test_epoch(), [ask_and_block_spec()]),
        10000,
        make_deps(mock, 0, nil)
      )

    assert outcome.kind == :defer
    assert length(outcome.specifications) == 1
    [decision] = outcome.specifications
    assert decision.specification_id == "spec-1"
    assert decision.unresolved_reason == :UNRESOLVED_REASON_EVIDENCE_GAP
  end

  test "per-spec decisions survive into a fail-mode outcome too" do
    reason = :UNRESOLVED_REASON_TIMEOUT

    mock =
      MockDecider.new(
        response:
          make_response(:DECISION_ACTION_UNSPECIFIED, [
            make_decision("spec-1", :DECISION_ACTION_UNSPECIFIED, reason)
          ])
      )

    outcome =
      gate(
        make_event(test_kind()),
        make_filter(test_epoch(), [ask_and_block_spec()]),
        10000,
        make_deps(mock, 0, nil)
      )

    assert outcome.kind == :fail_open_permit
    assert length(outcome.specifications) == 1
    assert hd(outcome.specifications).unresolved_reason == :UNRESOLVED_REASON_TIMEOUT
  end

  test "a deadline transport error is classified distinctly from a Connect code" do
    mock = MockDecider.new(error: %Transport.Error{kind: :deadline, message: "host gave up"})

    outcome =
      gate(
        make_event(test_kind()),
        make_filter(test_epoch(), [ask_and_block_spec()]),
        nil,
        make_deps(mock, 0, nil)
      )

    assert outcome.kind == :fail_open_permit
    assert String.starts_with?(outcome.reason, "context-deadline-exceeded")
  end

  test "a Connect error is classified with its code for auditability" do
    mock =
      MockDecider.new(
        error: %Transport.Error{kind: :connect, code: :unavailable, message: "endpoint down"}
      )

    outcome =
      gate(
        make_event(test_kind()),
        make_filter(test_epoch(), [ask_and_block_spec()]),
        10000,
        make_deps(mock, 0, nil)
      )

    assert outcome.kind == :fail_open_permit
    assert String.starts_with?(outcome.reason, "connect-unavailable")
  end

  test "kind_to_string renders the hyphenated audit form" do
    assert Gate.kind_to_string(:permit) == "permit"
    assert Gate.kind_to_string(:deny) == "deny"
    assert Gate.kind_to_string(:defer) == "defer"
    assert Gate.kind_to_string(:fail_open_permit) == "fail-open-permit"
    assert Gate.kind_to_string(:fail_closed_deny) == "fail-closed-deny"
    assert Gate.kind_to_string(:no_filter) == "no-filter"
    assert Gate.kind_to_string(:unspecified) == "unspecified"
  end

  test "the zero/unspecified kind is never permitted" do
    refute Gate.permitted?(:unspecified)
  end

  test "missing eligible budget exhausts without clock or Decide" do
    mock = MockDecider.new()
    spec = %{ask_and_block_spec() | latency_budget_nanoseconds: nil}
    Process.put(:budget_clock_calls, 0)

    deps = %Gate.Deps{
      decide: fn req -> MockDecider.decide(mock, req) end,
      now_monotonic_ns: fn ->
        Process.put(:budget_clock_calls, Process.get(:budget_clock_calls, 0) + 1)
        0
      end,
      accepted_fail_mode_for: fn _ -> :FAIL_MODE_OPEN end
    }

    outcome = gate(make_event(test_kind()), make_filter(test_epoch(), [spec]), nil, deps)
    assert outcome.kind == :fail_open_permit
    assert Process.get(:budget_clock_calls) == 0
    assert MockDecider.call_count() == 0
  end
end
