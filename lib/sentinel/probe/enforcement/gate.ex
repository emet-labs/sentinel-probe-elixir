defmodule Sentinel.Probe.SDK.Enforcement.Gate do
  @moduledoc """
  The one runtime enforcement gate (ADR-0023 gate 2). Elixir analog of
  `sdk/go/enforcement/gate.go` (`enforcement-gate.ts`).

  The gate enforces one `ASK_AND_BLOCK` event and returns the action the Probe
  must take. Control flow mirrors the reference step for step:

    1. no filter, or a filter with no epoch, returns `:no_filter` without asking;
    2. the enforcing set is the specs that select the event AND declare
       `ASK_AND_BLOCK` delivery; an empty set means the event is ship-async and
       the gate is a no-op permit;
    3. the aggregate fail mode is `:FAIL_MODE_CLOSED` iff some enforcing spec
       declares `CLOSED` AND the deployment has accepted `CLOSED` for it —
       fail-closed wins over any number of open specs;
    4. if a deadline was set and the budget is already exhausted, apply the fail
       mode WITHOUT asking;
    5. build the `DecideRequest` from the event the caller passed, which the
       caller has already projected through `Sentinel.Probe.SDK.Filter` — the
       gate never projects;
    6. ask; any error applies the aggregate fail mode;
    7. `PERMIT` permits, `DENY` denies, `DEFER` defers while budget remains (or
       no deadline was set) and otherwise applies the fail mode, and
       `UNSPECIFIED` applies the fail mode — never a blind permit.

  The gate holds no mutable state and is safe to call from many processes.

  ## Divergence from the Go analog (no BEAM context)

  Go takes a `context.Context` and classifies a dead context as a transport
  error that routes into the fail mode WITHOUT asking (D17). BEAM has no
  per-call cancellation context threaded through this gate; timeouts are owned by
  the Finch transport (a `:deadline` transport error) and the injected
  `deadline_ns`. So this gate takes no context, and `describe_error/1` classifies
  transport errors (`:connect`, `:deadline`, `:transport`) plus generic terms.
  """

  alias Sentinel.Model.V1.{EventFilter, FailMode, ProducerEvent, SpecificationFilter}
  alias Sentinel.Probe.V1.{DecideRequest, DecideResponse, SpecificationDecision}

  alias Sentinel.Probe.SDK.Client.Transport
  alias Sentinel.Probe.SDK.Enforcement.Budget
  alias Sentinel.Probe.SDK.Internal.SpecMatch

  # The outcome discriminant. A `:no_filter` of its own so an auditor can tell
  # "we had no policy" from "we had policy and could not reach Sentinel".
  @type outcome_kind ::
          :unspecified
          | :permit
          | :deny
          | :defer
          | :fail_open_permit
          | :fail_closed_deny
          | :no_filter

  @type t :: %__MODULE__{
          kind: outcome_kind(),
          reason: String.t(),
          filter_epoch: non_neg_integer() | nil,
          specifications: [SpecificationDecision.t()]
        }

  defstruct [:kind, :reason, :filter_epoch, specifications: []]

  @doc """
  Reports whether the Probe may proceed.

  Both `:permit` and `:fail_open_permit` allow the action; `:deny`,
  `:fail_closed_deny` and `:defer` do not. `:no_filter` permits, matching the
  reference's conservative default of not blocking on absent policy. `:unspecified`
  (the zero value a caller forgot to fill in) does NOT — it must never read as
  permitted.
  """
  @spec permitted?(outcome_kind()) :: boolean()
  def permitted?(:permit), do: true
  def permitted?(:fail_open_permit), do: true
  def permitted?(:no_filter), do: true
  def permitted?(_), do: false

  @doc """
  Renders a kind as the audit/`GateOutcome`-string form (hyphenated, matching the
  Go analog's `String()`), so failure messages and audit records are readable and
  stable across SDKs.
  """
  @spec kind_to_string(outcome_kind()) :: String.t()
  def kind_to_string(:unspecified), do: "unspecified"
  def kind_to_string(:permit), do: "permit"
  def kind_to_string(:deny), do: "deny"
  def kind_to_string(:defer), do: "defer"
  def kind_to_string(:fail_open_permit), do: "fail-open-permit"
  def kind_to_string(:fail_closed_deny), do: "fail-closed-deny"
  def kind_to_string(:no_filter), do: "no-filter"

  @typedoc """
  The effects the gate needs, all injected so the gate itself stays pure and
  testable without a network or a clock.

    * `:decide` — performs the ask. In production this wraps
      `Sentinel.Probe.SDK.Client.Transport`; in tests it is a stub. Returns
      `{:ok, DecideResponse.t()}` | `{:error, term}`.
    * `:now_monotonic_ns` — reads the host's monotonic clock. Required whenever
      `deadline_ns` is set.
    * `:accepted_fail_mode_for` — reports the fail mode the deployment has
      actually contracted for a spec. A spec declaring `CLOSED` without an
      accepted contract downgrades to `OPEN`: an operator must have agreed to be
      blocked. REQUIRED whenever an enforcing Specification selects the event;
      the gate raises when it is `nil` (see `compute_aggregate_fail_mode/2`).
  """

  # Deps and Options are defined in their own submodules below to keep each
  # defstruct in its own module (Elixir allows one defstruct per module).

  @type deps :: %__MODULE__.Deps{
          decide: (DecideRequest.t() -> {:ok, DecideResponse.t()} | {:error, term}),
          now_monotonic_ns: (-> integer()),
          accepted_fail_mode_for: (SpecificationFilter.t() -> FailMode.t())
        }

  defmodule Deps do
    @moduledoc false
    defstruct [:decide, :now_monotonic_ns, :accepted_fail_mode_for]
  end

  @typedoc "Per-call identifiers stamped into the `DecideRequest`."
  @type options :: %__MODULE__.Options{
          request_id: String.t(),
          idempotency_key: String.t(),
          source_handle: String.t()
        }

  defmodule Options do
    @moduledoc false
    defstruct [:request_id, :idempotency_key, :source_handle]
  end

  @doc """
  Enforces one `ASK_AND_BLOCK` event and returns the action the Probe must take.

  `deadline_ns` is an ABSOLUTE monotonic deadline (caller computes
  `now_monotonic_ns + latency_budget_ns`); `nil` means no latency budget was
  declared. The wire field `remaining_transport_budget_nanoseconds` is a derived
  RELATIVE value. `filter` is the caller's snapshot; the gate holds no state.
  """
  @spec gate(
          event :: ProducerEvent.t() | nil,
          filter :: EventFilter.t() | nil,
          deadline_ns :: integer() | nil,
          deps :: deps(),
          options :: options()
        ) :: t()
  def gate(event, filter, deadline_ns, deps, options) do
    # 1. No-filter guard. Presence is "epoch is nil": epoch 0 is a legitimate
    #    epoch, so `epoch == 0` would misclassify it as "no policy held".
    if filter == nil or is_nil(filter.epoch) do
      %__MODULE__{kind: :no_filter, reason: "no-filter"}
    else
      enforce(event, filter, deadline_ns, deps, options)
    end
  end

  defp enforce(event, %EventFilter{} = filter, deadline_ns, deps, options) do
    filter_epoch = filter.epoch

    # 2. Enforcing set: selects the event AND asks-and-blocks.
    enforcing =
      Enum.filter(filter.specifications, fn spec ->
        SpecMatch.selects?(spec, event) and enforceable?(spec)
      end)

    if enforcing == [] do
      %__MODULE__{
        kind: :permit,
        reason: "no-ask-and-block-spec",
        filter_epoch: filter_epoch
      }
    else
      # 3. Aggregate fail mode: fail-closed wins.
      aggregate_fail_mode = compute_aggregate_fail_mode(enforcing, deps)

      budgets = Enum.map(enforcing, & &1.latency_budget_nanoseconds)
      if Enum.any?(budgets, &(&1 in [nil, 0])) do
        apply_fail_mode(aggregate_fail_mode, "budget-exhausted", filter_epoch, nil)
      else
        anchor = deps.now_monotonic_ns.()
        specification_budget = Enum.min(budgets)
        caller_budget =
          if deadline_ns == nil,
            do: specification_budget,
            else: Budget.monotonic_delta_ns(anchor, deadline_ns) || 0
        effective_budget = min(specification_budget, caller_budget)
        remaining = Budget.remaining_budget_ns(anchor, effective_budget, deps.now_monotonic_ns.())
        if remaining == 0 do
          apply_fail_mode(aggregate_fail_mode, "budget-exhausted", filter_epoch, nil)
        else
          ask(
            event,
            filter_epoch,
            {anchor, effective_budget},
            aggregate_fail_mode,
            remaining,
            deps,
            options
          )
        end
      end
    end
  end

  defp ask(event, filter_epoch, budget_state, aggregate_fail_mode, remaining_budget, deps, options) do
    # 5. Build the request from the already-projected event.
    request = %DecideRequest{
      request_id: options.request_id,
      idempotency_key: options.idempotency_key,
      source_handle: options.source_handle,
      filter_epoch: filter_epoch,
      producer_event: event,
      remaining_transport_budget_nanoseconds: remaining_budget
    }

    # 6. Ask. Any error routes into the fail mode, never a permit, never a raise.
    case deps.decide.(request) do
      {:ok, response} ->
        handle_response(response, aggregate_fail_mode, filter_epoch, budget_state, deps)

      {:error, error} ->
        apply_fail_mode(aggregate_fail_mode, describe_error(error), filter_epoch, nil)
    end
  end

  defp handle_response(response, aggregate_fail_mode, filter_epoch, deadline_ns, deps) do
    decisions = response.specifications

    case response.action do
      :DECISION_ACTION_PERMIT ->
        %__MODULE__{
          kind: :permit,
          reason: "permit",
          filter_epoch: filter_epoch,
          specifications: decisions
        }

      :DECISION_ACTION_DENY ->
        %__MODULE__{
          kind: :deny,
          reason: "deny",
          filter_epoch: filter_epoch,
          specifications: decisions
        }

      :DECISION_ACTION_DEFER ->
        {anchor, budget} = deadline_ns
        if Budget.remaining_budget_ns(anchor, budget, deps.now_monotonic_ns.()) > 0 do
          %__MODULE__{
            kind: :defer,
            reason: "defer",
            filter_epoch: filter_epoch,
            specifications: decisions
          }
        else
          apply_fail_mode(aggregate_fail_mode, "defer-budget-exhausted", filter_epoch, decisions)
        end

      :DECISION_ACTION_UNSPECIFIED ->
        apply_fail_mode(aggregate_fail_mode, "unspecified-action", filter_epoch, decisions)

      _other ->
        # A future action this Probe does not understand is unresolved, not permitted.
        apply_fail_mode(aggregate_fail_mode, "unspecified-action", filter_epoch, decisions)
    end
  end

  defp apply_fail_mode(fail_mode, reason, filter_epoch, decisions) do
    kind = if fail_mode == :FAIL_MODE_CLOSED, do: :fail_closed_deny, else: :fail_open_permit

    %__MODULE__{kind: kind, reason: reason, filter_epoch: filter_epoch, specifications: decisions}
  end

  @doc """
  Returns `:FAIL_MODE_CLOSED` iff some enforcing spec both DECLARES `CLOSED` and
  has `CLOSED` accepted by the deployment. Declaration alone downgrades to
  `OPEN`: blocking a caller is something an operator has to have agreed to.
  `:FAIL_MODE_UNSPECIFIED` is `OPEN`.

  Raises when `deps.accepted_fail_mode_for` is `nil`. A missing contract source
  is a wiring bug, and defaulting it to `OPEN` would silently disable fail-closed
  for every Specification — the exact failure this package exists to prevent. It
  is only reached when the enforcing set is non-empty, so a Probe with no
  ask-and-block Specifications never needs the dependency.
  """
  @spec compute_aggregate_fail_mode([SpecificationFilter.t()], deps()) :: FailMode.t()
  def compute_aggregate_fail_mode(_enforcing, %Deps{accepted_fail_mode_for: nil}) do
    raise "enforcement: Deps.AcceptedFailModeFor is required when an enforcing " <>
            "Specification selects the event; a nil contract source cannot be defaulted to " <>
            "fail-open without silently disabling fail-closed enforcement"
  end

  def compute_aggregate_fail_mode(enforcing, %Deps{accepted_fail_mode_for: accepted}) do
    Enum.reduce_while(enforcing, :FAIL_MODE_OPEN, fn spec, _acc ->
      if spec.fail_mode == :FAIL_MODE_CLOSED and accepted.(spec) == :FAIL_MODE_CLOSED do
        {:halt, :FAIL_MODE_CLOSED}
      else
        {:cont, :FAIL_MODE_OPEN}
      end
    end)
  end

  defp enforceable?(%{event_match: %{delivery_mode: :DELIVERY_MODE_ASK_AND_BLOCK}, evaluation_mode: :EVALUATION_MODE_ENFORCE, readiness: :READINESS_ACTIVE}), do: true
  defp enforceable?(_), do: false

  @doc """
  Renders a transport failure for the audit record, distinguishing a deadline
  from a Connect status so an operator can tell "the host gave up" from "the
  endpoint said no". Every error class still routes into the fail mode.
  """
  @spec describe_error(term()) :: String.t()
  def describe_error(%Transport.Error{kind: :deadline, message: message}) do
    "context-deadline-exceeded: " <> message
  end

  def describe_error(%Transport.Error{kind: :cancelled, message: message}) do
    "context-canceled: " <> message
  end

  def describe_error(%Transport.Error{kind: :connect, code: code, message: message}) do
    "connect-" <> Atom.to_string(code) <> ": " <> message
  end

  def describe_error(%Transport.Error{kind: :transport, message: message}) do
    "transport-error: " <> message
  end

  def describe_error(%{__exception__: true} = exception) do
    "transport-error: " <> Exception.message(exception)
  end

  def describe_error(error) when is_binary(error) do
    "transport-error: " <> error
  end

  def describe_error(error) do
    "transport-error: " <> inspect(error)
  end
end
