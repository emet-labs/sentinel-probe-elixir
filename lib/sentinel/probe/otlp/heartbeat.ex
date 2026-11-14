defmodule Sentinel.Probe.SDK.Otlp.Heartbeat do
  @moduledoc """
  Reports a `Sentinel.Probe.SDK.Emission.Sequence` declaration to Sentinel
  (#461).

  Each report builds one `ProducerCompletenessSignal` from the allocator's
  `declare/1` at the moment it is sent: `last_emitted`, `complete_through` and
  `acknowledged_filter_epoch` always set (zero included, so presence is on the
  wire), and the producer clock domain `"unix"` with zero uncertainty. Between
  two reports with nothing new, every field except `producer_reported_at`'s
  nanoseconds is identical, so Sentinel sees a restatement and writes no Verdict
  revision.

  Guarantees, in order of importance:

    * `complete_through` comes from the allocator, so it never runs ahead of a
      successful export;
    * a report never regresses the epoch, the counters or the acknowledged
      Filter epoch, and never advances a declared close (the allocator opens a
      new epoch instead);
    * before the first accepted signal of a new epoch, the previous epoch's
      final declaration is restated once per report, so a close whose post
      failed, or a Filter acknowledgement that arrived while the epoch was
      closed, is not lost at the roll.

  Reports run one at a time. The sends run in a task, so `declare_idle/1` and
  `declare_active/1` answer while a send is in flight. Every report requested
  while one is sending (a call or a `:tick`) coalesces into one follow-up, built
  after it.

  ## Options

    * `:sequence` (required) — the allocator.
    * `:send` — `(signal -> :ok | {:ok, term} | {:error, term})`. Anything but
      `:ok` or `{:ok, _}`, or a raise, is a signal Sentinel did not accept.
    * `:otlp` — `%{endpoint: origin, source_handle: handle}`; the default send
      posts to `/v1/logs` through `Sentinel.Probe.SDK.Otlp.Exporter`. One of
      `:send` and `:otlp` is required.
    * `:filter_epoch` — `(-> integer | nil)`, the acknowledged Filter epoch, for
      example `fn -> FilterStore.epoch(store) end`. `nil` is sent as `0`.
    * `:now` — `(-> integer)` Unix nanoseconds. Defaults to
      `System.os_time(:nanosecond)`.
    * `:interval_ms` — `0` (no timer, `report/1` only) or `1000..59999`.
      Defaults to `15000`.
    * `:schedule` — `(pid, ms -> reference)` arms the next `:tick`. Defaults to
      `Process.send_after(pid, :tick, ms)`. Tests send `:tick` themselves.
    * `:on_error` — `(term -> term)` receives every refused send, a
      `%SequenceError{code: "filter-epoch-regressed"}` for a suppressed report,
      and nothing else. Defaults to a no-op.
    * `:name` — a GenServer name.

  `stop/1` silences the heartbeat without sending (a process that must go
  quiet). `shutdown/1` waits for a running report, sends one final report, then
  silences it; the final signal is a close only if nothing is unresolved. Either
  way the process stays up and inert until its supervisor or
  `GenServer.stop/1` ends it.
  """

  use GenServer

  alias Sentinel.Model.V1.{OccurrenceTime, ProducerCompletenessSignal}
  alias Sentinel.Probe.SDK.Emission.{Sequence, SequenceError}
  alias Sentinel.Probe.SDK.Int128
  alias Sentinel.Probe.SDK.Otlp.Exporter

  @default_interval_ms 15_000
  @min_interval_ms 1_000
  # Below the 60 s active-silence bound.
  @max_interval_ms 59_999

  @type report :: %{
          sequence_epoch: non_neg_integer(),
          last_emitted: non_neg_integer(),
          complete_through: non_neg_integer(),
          acknowledged_filter_epoch: non_neg_integer(),
          accepted: boolean()
        }

  defstruct [
    :sequence,
    :send,
    :filter_epoch,
    :now,
    :interval_ms,
    :schedule,
    :on_error,
    :activity_since,
    timer: nil,
    activity: :active,
    idle_mark: nil,
    last_reported_at: 0,
    last_accepted: nil,
    ack_floor: %{},
    stopped: false,
    running: nil,
    waiting: [],
    follow_up: false,
    closing: nil
  ]

  @doc "The default report interval in milliseconds."
  @spec default_interval_ms :: pos_integer()
  def default_interval_ms, do: @default_interval_ms

  @doc "Starts a heartbeat. Raises `ArgumentError` for invalid options."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    sequence = Keyword.get(opts, :sequence) || raise ArgumentError, ":sequence is required"
    interval = Keyword.get(opts, :interval_ms, @default_interval_ms)

    unless interval == 0 or
             (is_integer(interval) and interval in @min_interval_ms..@max_interval_ms) do
      raise ArgumentError,
            "interval_ms must be 0 or #{@min_interval_ms}..#{@max_interval_ms}, got: " <>
              inspect(interval)
    end

    config = %{
      sequence: sequence,
      send: send_function(opts),
      filter_epoch: Keyword.get(opts, :filter_epoch, fn -> nil end),
      now: Keyword.get(opts, :now, fn -> System.os_time(:nanosecond) end),
      interval_ms: interval,
      schedule: Keyword.get(opts, :schedule, &Process.send_after(&1, :tick, &2)),
      on_error: Keyword.get(opts, :on_error, fn _ -> :ok end)
    }

    GenServer.start_link(__MODULE__, config, Keyword.take(opts, [:name]))
  end

  @doc """
  Sends the current declaration and returns one report per signal sent. A call
  made while another report is sending waits for, and returns, the one
  follow-up report built after it.
  """
  @spec report(GenServer.server(), timeout()) :: [report()]
  def report(server, timeout \\ :infinity), do: GenServer.call(server, :report, timeout)

  @doc "Declares the source INTENTIONALLY_IDLE. The next allocation ends it."
  @spec declare_idle(GenServer.server()) :: :ok
  def declare_idle(server), do: GenServer.call(server, :declare_idle)

  @doc "Declares the source ACTIVE again."
  @spec declare_active(GenServer.server()) :: :ok
  def declare_active(server), do: GenServer.call(server, :declare_active)

  @doc "Cancels the schedule and silences the heartbeat. Sends nothing."
  @spec stop(GenServer.server()) :: :ok
  def stop(server), do: GenServer.call(server, :stop)

  @doc """
  Cancels the schedule, waits for a running report, sends one final report and
  silences the heartbeat. Returns the final report.
  """
  @spec shutdown(GenServer.server(), timeout()) :: [report()]
  def shutdown(server, timeout \\ :infinity), do: GenServer.call(server, :shutdown, timeout)

  @doc """
  Builds the wire signal from `:sequence_epoch`, `:last_emitted`,
  `:complete_through`, `:acknowledged_filter_epoch`, `:activity` (`:active` or
  `:idle`), `:reported_at_ns` and `:activity_since_ns`.
  """
  @spec build_signal(map()) :: ProducerCompletenessSignal.t()
  def build_signal(%{} = fields) do
    %ProducerCompletenessSignal{
      sequence_epoch: fields.sequence_epoch,
      last_emitted: fields.last_emitted,
      complete_through: fields.complete_through,
      producer_reported_at: unix_time(fields.reported_at_ns),
      acknowledged_filter_epoch: fields.acknowledged_filter_epoch,
      activity: activity_state(fields.activity),
      activity_since: unix_time(fields.activity_since_ns)
    }
  end

  @impl true
  def init(config) do
    state = struct!(__MODULE__, Map.put(config, :activity_since, config.now.()))
    {:ok, arm(state)}
  end

  @impl true
  def handle_call(:report, _from, %{stopped: true} = state), do: {:reply, [], state}

  def handle_call(:report, from, %{running: nil} = state),
    do: {:noreply, start_run(state, [from], false)}

  def handle_call(:report, from, state),
    do: {:noreply, %{state | waiting: [from | state.waiting]}}

  def handle_call(:declare_idle, _from, %{activity: :idle} = state), do: {:reply, :ok, state}

  def handle_call(:declare_idle, _from, state) do
    declaration = Sequence.declaration(state.sequence)

    state = %{
      state
      | activity: :idle,
        activity_since: max(state.now.(), state.last_reported_at),
        idle_mark: {declaration.epoch, declaration.last_emitted}
    }

    {:reply, :ok, state}
  end

  def handle_call(:declare_active, _from, %{activity: :active} = state), do: {:reply, :ok, state}

  def handle_call(:declare_active, _from, state) do
    since = max(state.now.(), state.last_reported_at)
    {:reply, :ok, %{state | activity: :active, activity_since: since}}
  end

  def handle_call(:stop, _from, state) do
    state = cancel_timer(state)

    state =
      case state.running do
        nil ->
          state

        %{task: task} = running ->
          case Task.shutdown(task, :brutal_kill) do
            {:ok, results} -> finish(state, running, results)
            _ -> reply_all(%{state | running: nil}, running.callers, [])
          end
      end

    state = %{state | stopped: true, running: nil}
    state = reply_all(state, waiting_callers(state), [])
    {:reply, :ok, %{state | waiting: [], closing: nil, follow_up: false}}
  end

  def handle_call(:shutdown, _from, %{stopped: true} = state), do: {:reply, [], state}

  def handle_call(:shutdown, from, %{running: nil} = state),
    do: {:noreply, start_run(cancel_timer(state), [from], true)}

  def handle_call(:shutdown, from, state),
    do: {:noreply, %{cancel_timer(state) | closing: from}}

  @impl true
  def handle_info(:tick, %{stopped: true} = state), do: {:noreply, state}

  # A tick already queued when shutdown cancelled the timer neither re-arms it
  # nor adds a report after the final one.
  def handle_info(:tick, %{closing: closing} = state) when closing != nil, do: {:noreply, state}
  def handle_info(:tick, %{running: %{final: true}} = state), do: {:noreply, state}

  def handle_info(:tick, state) do
    state = arm(state)

    case state.running do
      nil -> {:noreply, start_run(state, [], false)}
      _ -> {:noreply, %{state | follow_up: true}}
    end
  end

  def handle_info({ref, results}, %{running: %{task: %Task{ref: ref}} = running} = state) do
    Process.demonitor(ref, [:flush])
    {:noreply, state |> finish(running, results) |> next()}
  end

  def handle_info(_message, state), do: {:noreply, state}

  # Plans one report: declare, fix the report instant, choose the signals, and
  # hand the sends to a task. A report that sends nothing finishes at once.
  defp start_run(state, callers, final) do
    declaration = Sequence.declare(state.sequence)
    ack = state.filter_epoch.() || 0
    state = advance_clock(state, declaration)
    {sends, state} = plan(state, declaration, ack)
    running = %{callers: callers, final: final}

    case sends do
      [] ->
        state |> finish(running, []) |> next()

      _ ->
        send = state.send
        task = Task.async(fn -> Enum.map(sends, fn {d, a, s} -> {d, a, deliver(send, s)} end) end)
        %{state | running: Map.put(running, :task, task)}
    end
  end

  # The report instant never goes before an earlier report or the activity
  # change; an allocation since an idle declaration ends it.
  defp advance_clock(state, declaration) do
    now = Enum.max([state.now.(), state.last_reported_at, state.activity_since])
    state = %{state | last_reported_at: now}

    if state.activity == :idle and state.idle_mark != {declaration.epoch, declaration.last_emitted},
      do: %{state | activity: :active, activity_since: now},
      else: state
  end

  defp plan(state, declaration, ack) do
    previous = declaration.previous
    current = Map.take(declaration, [:epoch, :last_emitted, :complete_through])

    final =
      if previous != nil and accepted_epoch(state.last_accepted) != declaration.epoch and
           state.last_accepted != {previous, ack} and ack >= floor_of(state, previous.epoch),
         do: [previous],
         else: []

    floor = floor_of(state, declaration.epoch)

    targets =
      if ack < floor do
        state.on_error.(
          SequenceError.new(
            "filter-epoch-regressed",
            "acknowledged Filter epoch #{ack} is below #{floor} already sent for epoch " <>
              "#{declaration.epoch}"
          )
        )

        final
      else
        final ++ [current]
      end

    sends = Enum.map(targets, &{&1, ack, signal(state, &1, ack)})
    oldest = if previous, do: previous.epoch, else: declaration.epoch

    ack_floor =
      targets
      |> Enum.reduce(state.ack_floor, fn d, floors ->
        Map.update(floors, d.epoch, ack, &max(&1, ack))
      end)
      |> Map.reject(fn {epoch, _} -> epoch < oldest end)

    {sends, %{state | ack_floor: ack_floor}}
  end

  defp signal(state, declaration, ack) do
    build_signal(%{
      sequence_epoch: declaration.epoch,
      last_emitted: declaration.last_emitted,
      complete_through: declaration.complete_through,
      acknowledged_filter_epoch: ack,
      activity: state.activity,
      reported_at_ns: state.last_reported_at,
      activity_since_ns: state.activity_since
    })
  end

  # Applies the outcomes in send order and answers the report's callers.
  defp finish(state, running, results) do
    {reports, state} =
      Enum.map_reduce(results, state, fn {declaration, ack, outcome}, state ->
        state =
          case outcome do
            :accepted ->
              %{state | last_accepted: {declaration, ack}}

            {:not_accepted, reason} ->
              state.on_error.(reason)
              state
          end

        {%{
           sequence_epoch: declaration.epoch,
           last_emitted: declaration.last_emitted,
           complete_through: declaration.complete_through,
           acknowledged_filter_epoch: ack,
           accepted: outcome == :accepted
         }, state}
      end)

    state = reply_all(%{state | running: nil}, running.callers, reports)
    if running.final, do: %{state | stopped: true}, else: state
  end

  # After a report: a requested shutdown runs the final report, serving every
  # waiting caller too; otherwise waiting callers and ticks share one follow-up.
  defp next(%{stopped: true} = state) do
    state = reply_all(state, waiting_callers(state), [])
    %{state | waiting: [], closing: nil, follow_up: false}
  end

  defp next(%{closing: closing} = state) when closing != nil do
    callers = Enum.reverse([closing | state.waiting])
    start_run(%{state | waiting: [], closing: nil, follow_up: false}, callers, true)
  end

  defp next(%{waiting: [], follow_up: false} = state), do: state

  defp next(state) do
    callers = Enum.reverse(state.waiting)
    start_run(%{state | waiting: [], follow_up: false}, callers, false)
  end

  defp deliver(send, signal) do
    case send.(signal) do
      :ok -> :accepted
      {:ok, _} -> :accepted
      {:error, reason} -> {:not_accepted, reason}
      other -> {:not_accepted, {:unexpected_send_result, other}}
    end
  rescue
    error -> {:not_accepted, error}
  catch
    kind, reason -> {:not_accepted, {kind, reason}}
  end

  defp waiting_callers(state) do
    if state.closing, do: [state.closing | state.waiting], else: state.waiting
  end

  defp reply_all(state, callers, reply) do
    Enum.each(callers, &GenServer.reply(&1, reply))
    state
  end

  defp accepted_epoch(nil), do: nil
  defp accepted_epoch({declaration, _ack}), do: declaration.epoch

  defp floor_of(state, epoch), do: Map.get(state.ack_floor, epoch, 0)

  defp arm(%{interval_ms: 0} = state), do: state
  defp arm(state), do: %{state | timer: state.schedule.(self(), state.interval_ms)}

  defp cancel_timer(state) do
    if is_reference(state.timer), do: Process.cancel_timer(state.timer)
    %{state | timer: nil}
  end

  defp send_function(opts) do
    case {Keyword.get(opts, :send), Keyword.get(opts, :otlp)} do
      {send, _} when is_function(send, 1) ->
        send

      {nil, %{endpoint: endpoint, source_handle: handle}}
      when is_binary(endpoint) and is_binary(handle) ->
        fn signal -> Exporter.export_completeness(endpoint, handle, signal) end

      _ ->
        raise ArgumentError, "a :send function or an :otlp endpoint and source handle is required"
    end
  end

  defp unix_time(nanos) do
    %OccurrenceTime{
      clock_domain_id: "unix",
      nanoseconds: Int128.from_int64(nanos),
      uncertainty_nanoseconds: 0
    }
  end

  defp activity_state(:active), do: :SOURCE_ACTIVITY_STATE_ACTIVE
  defp activity_state(:idle), do: :SOURCE_ACTIVITY_STATE_INTENTIONALLY_IDLE
end
