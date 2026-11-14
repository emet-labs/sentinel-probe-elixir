defmodule Sentinel.Probe.SDK.Otlp.AsyncExporter do
  @moduledoc """
  A supervised, bounded, asynchronous trace exporter with retries. It is the
  exporter `Sentinel.Probe.SDK.Emission.Processor` uses by default.

  The span end path only enqueues (`submit/2` is a cast): a Sentinel outage
  cannot block a Kafka consumer or an HTTP handler, and nothing here raises into
  the caller. A worker process does the HTTP, one batch at a time, so this
  GenServer never blocks on the network and a crash in a send cannot take it
  down.

  ## Slot outcomes

  Every submitted item carries its Sequence slot. The slot is marked:

    * exported, when Sentinel accepted the span (HTTP 2xx, zero `rejected_spans`);
    * failed, in every other case, so `complete_through` never passes it.

  ## Bounded queue and overflow policy

  At most `:max_queue` items wait. When the queue is full the NEW item is
  dropped: its slot is marked failed, `:on_error` receives
  `{:queue_overflow, slot}` and the item is kept for a later retry (see below).
  Dropping an Event is never silent and never a pass: the epoch stays open below
  that slot.

  ## Retries

  The Probe SDK retry policy (`sdk/README.md`, the same in every Probe SDK): a
  batch is retried with exponential backoff and jitter (`:backoff_ms`, doubling
  to `:max_backoff_ms`, each wait 50-100% of its step) only for transient
  failures: a transport error and HTTP 408, 425, 429, 500, 502, 503 or 504. A
  `Retry-After` on a 429 or 503 raises the wait, never past `:max_backoff_ms`.
  After `:max_attempts` the slots are failed. Anything else (an encode error,
  any other HTTP status, a rejected span, an undecodable response) is permanent
  for that span and is not retried. When a multi-span batch fails permanently
  it is re-sent one span at a time so one bad span cannot fail its neighbours.
  A slot is marked exported only after a 2xx with zero `rejected_spans`, so
  `complete_through` never passes a failed or in-flight export.

  ## Duplicate Event id policy

  When Sentinel refuses a second delivery of an Event id it already holds
  (`evidence_conflict` or `identity_conflict` in the `error_message`),
  `:on_error` receives `{:duplicate_event_id, message}` (the reason
  `duplicate-event-id`), the slot stays FAILED, `complete_through` never passes
  it and nothing is retried: it is neither re-sent nor retained. The slot
  number never reached Sentinel as an Event, so completeness through it would be
  overstated; missing evidence never looks complete. Any other rejection
  (`slot_conflict`, an admission refusal, a closed boundary) is
  `{:rejected_spans, n}` and also stays FAILED.

  Failed slots that are worth retrying (transient failures and overflow drops;
  at most `:max_retained_failed`, oldest evicted) are re-sent every
  `:retry_interval_ms` and at `flush/2`, with one attempt each. A success marks
  the slot exported, which lets `complete_through` advance.

  ## Options

    * `:endpoint`, `:source_handle` (or `:otlp` as `%{endpoint:, source_handle:}`) — required.
    * `:sequence` — the allocator whose slots are marked. Without it nothing is marked.
    * `:resource_attributes` — `[{key, value}]` OTLP Resource attributes.
    * `:on_error` — `(term -> term)` for each failure; a raise inside it is ignored.
    * `:on_event` — `(ProducerEvent.t -> term)` called after Sentinel accepted the Event.
    * `:max_queue` (2048), `:batch_size` (64), `:max_attempts` (5), `:backoff_ms` (100),
      `:max_backoff_ms` (5000), `:retry_interval_ms` (30000, `0` disables the timer),
      `:max_retained_failed` (1024), `:log_interval_ms` (10000).
    * `:sleep` — `(non_neg_integer -> term)`, the wait between attempts. Defaults to
      `Process.sleep/1`; a test supplies its own.
    * `:post` — `(endpoint, handle, spans, opts -> {:ok, body} | {:error, term})`, the transport.
      Defaults to `Sentinel.Probe.SDK.Otlp.Exporter.export_traces/4`.
    * `:name` — a GenServer name.

  ## Shutdown

  `shutdown/2` drains the queue, retries failed slots once, stops accepting
  and, with `heartbeat: pid`, then sends the final completeness report. Call it
  before the process exits so the final heartbeat states what Sentinel holds.
  """

  use GenServer

  alias Sentinel.Probe.SDK.Emission.Sequence
  alias Sentinel.Probe.SDK.Internal.ErrorLog
  alias Sentinel.Probe.SDK.Otlp.{Exporter, Heartbeat, Http, PartialSuccess}

  # The Probe SDK retry policy: HTTP statuses worth another attempt (a transport error is too).
  @retryable_statuses [408, 425, 429, 500, 502, 503, 504]

  @type item :: %{
          required(:span) => Sentinel.Probe.SDK.Emission.Span.t(),
          optional(:slot) => Sequence.slot() | nil,
          optional(:event) => term()
        }

  defstruct [
    :config,
    queue: :queue.new(),
    queue_len: 0,
    worker: nil,
    retained: [],
    waiters: [],
    flush_retried?: false,
    accepting?: true,
    timer: nil
  ]

  @doc "Starts the exporter. Raises `ArgumentError` for missing or invalid options."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    config = config!(opts)
    GenServer.start_link(__MODULE__, config, Keyword.take(opts, [:name]))
  end

  @doc false
  def child_spec(opts) do
    %{id: Keyword.get(opts, :name, __MODULE__), start: {__MODULE__, :start_link, [opts]}}
  end

  @doc """
  Enqueues an item without blocking. Never raises and never exits. A server
  that is not alive leaves the slot in flight, which holds `complete_through`
  back and is therefore safe.
  """
  @spec submit(GenServer.server(), item()) :: :ok
  def submit(server, %{span: _} = item) do
    GenServer.cast(server, {:submit, item})
  catch
    _, _ -> :ok
  end

  @doc """
  Waits until the queue is empty and the worker is idle, after one retry pass of
  the retained failed slots. `{:error, :timeout}` when `timeout` elapses first.
  """
  @spec flush(GenServer.server(), timeout()) :: :ok | {:error, term()}
  def flush(server, timeout \\ 5_000) do
    GenServer.call(server, :flush, timeout)
  catch
    :exit, {:timeout, _} -> {:error, :timeout}
    :exit, reason -> {:error, {:exporter_down, reason}}
  end

  @doc """
  Flushes, stops accepting new items (later items are failed), then, with
  `heartbeat: pid`, sends the final report. Options: `:timeout` (10000),
  `:heartbeat`. Returns the flush result; the heartbeat is sent either way.
  """
  @spec shutdown(GenServer.server(), keyword()) :: :ok | {:error, term()}
  def shutdown(server, opts \\ []) do
    result = flush(server, Keyword.get(opts, :timeout, 10_000))

    try do
      GenServer.call(server, :stop_accepting, 5_000)
    catch
      _, _ -> :ok
    end

    case Keyword.get(opts, :heartbeat) do
      nil -> :ok
      heartbeat -> Heartbeat.shutdown(heartbeat)
    end

    result
  end

  @doc "Queue depth, retained failed count and whether a send is running."
  @spec stats(GenServer.server()) :: %{
          queued: non_neg_integer(),
          retained: non_neg_integer(),
          sending: boolean()
        }
  def stats(server), do: GenServer.call(server, :stats)

  # -- GenServer ------------------------------------------------------------

  @impl true
  def init(config) do
    {:ok, schedule_retry(%__MODULE__{config: config})}
  end

  @impl true
  def handle_cast({:submit, item}, %{accepting?: false} = state) do
    {:noreply, fail_without_send(state, item, :exporter_stopped, false)}
  end

  def handle_cast({:submit, item}, state) do
    if state.queue_len >= state.config.max_queue do
      {:noreply, fail_without_send(state, item, {:queue_overflow, Map.get(item, :slot)}, true)}
    else
      state = %{state | queue: :queue.in(item, state.queue), queue_len: state.queue_len + 1}
      {:noreply, progress(state)}
    end
  end

  @impl true
  def handle_call(:flush, from, state) do
    {:noreply, progress(%{state | waiters: [from | state.waiters]})}
  end

  def handle_call(:stop_accepting, _from, state), do: {:reply, :ok, %{state | accepting?: false}}

  def handle_call(:stats, _from, state) do
    stats = %{
      queued: state.queue_len,
      retained: length(state.retained),
      sending: state.worker != nil
    }

    {:reply, stats, state}
  end

  @impl true
  def handle_info({:done, token, results}, %{worker: {_pid, mon, token, _kind}} = state) do
    Process.demonitor(mon, [:flush])
    {:noreply, state |> settle(results) |> Map.put(:worker, nil) |> progress()}
  end

  def handle_info({:DOWN, mon, :process, _pid, reason}, %{worker: {_, mon, _, {_, batch}}} = state) do
    results = Enum.map(batch, &{&1, {:transient, {:worker_crashed, reason}}})
    {:noreply, state |> settle(results) |> Map.put(:worker, nil) |> progress()}
  end

  def handle_info(:retry_failed, state) do
    state = %{state | timer: nil}
    state = if state.worker == nil and state.queue_len == 0, do: start_retry(state), else: state
    {:noreply, schedule_retry(state)}
  end

  def handle_info(_other, state), do: {:noreply, state}

  # -- queue driving --------------------------------------------------------

  defp progress(%{worker: nil, queue_len: len} = state) when len > 0 do
    {batch, rest} = take(state.queue, state.config.batch_size, [])
    dispatch(%{state | queue: rest, queue_len: state.queue_len - length(batch)}, batch, :send)
  end

  defp progress(%{worker: nil, waiters: [_ | _], flush_retried?: false} = state) do
    case start_retry(state) do
      %{worker: nil} = same -> finish_flush(same)
      started -> %{started | flush_retried?: true}
    end
  end

  defp progress(%{worker: nil, waiters: [_ | _]} = state), do: finish_flush(state)
  defp progress(state), do: state

  defp finish_flush(state) do
    Enum.each(state.waiters, &GenServer.reply(&1, :ok))
    %{state | waiters: [], flush_retried?: false}
  end

  defp take(queue, 0, acc), do: {Enum.reverse(acc), queue}

  defp take(queue, n, acc) do
    case :queue.out(queue) do
      {{:value, item}, rest} -> take(rest, n - 1, [item | acc])
      {:empty, queue} -> {Enum.reverse(acc), queue}
    end
  end

  defp start_retry(%{retained: []} = state), do: state

  defp start_retry(state) do
    {batch, kept} = state.retained |> Enum.reverse() |> Enum.split(state.config.batch_size)
    dispatch(%{state | retained: Enum.reverse(kept)}, batch, :retry)
  end

  defp dispatch(state, batch, kind) do
    parent = self()
    token = make_ref()
    config = state.config
    attempts = if kind == :retry, do: 1, else: config.max_attempts

    {pid, mon} =
      spawn_monitor(fn ->
        send(parent, {:done, token, deliver(batch, config, attempts)})
      end)

    %{state | worker: {pid, mon, token, {kind, batch}}}
  end

  # -- delivery (runs in the worker process) --------------------------------

  defp deliver(batch, config, attempts) do
    case send_with_retries(batch, config, attempts) do
      :ok ->
        Enum.map(batch, &{&1, :ok})

      {:transient, reason} ->
        Enum.map(batch, &{&1, {:transient, reason}})

      {:permanent, _reason} when length(batch) > 1 ->
        Enum.flat_map(batch, &deliver([&1], config, attempts))

      {:permanent, reason} ->
        Enum.map(batch, &{&1, {:permanent, reason}})
    end
  end

  defp send_with_retries(batch, config, attempts), do: attempt(batch, config, 1, attempts)

  defp attempt(batch, config, n, attempts) do
    post_result = post(batch, config)

    case classify(post_result) do
      :ok ->
        :ok

      {:transient, _} = transient when n >= attempts ->
        transient

      {:transient, _} ->
        config.sleep.(retry_delay(config, n, retry_after(post_result)))
        attempt(batch, config, n + 1, attempts)

      {:permanent, _} = permanent ->
        permanent
    end
  end

  defp post(batch, config) do
    spans = Enum.map(batch, & &1.span)
    opts = [resource_attributes: config.resource_attributes]
    config.post.(config.endpoint, config.source_handle, spans, opts)
  rescue
    error -> {:error, {:exception, error}}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp classify({:ok, body}) do
    case PartialSuccess.rejection(body) do
      {:ok, 0, _message} ->
        :ok

      {:ok, rejected, message} ->
        if PartialSuccess.duplicate_event_id?(message),
          do: {:permanent, {:duplicate_event_id, message}},
          else: {:permanent, {:rejected_spans, rejected}}

      {:error, reason} ->
        {:permanent, {:undecodable_response, reason}}
    end
  end

  defp classify({:error, {:transport, _} = reason}), do: {:transient, reason}

  defp classify({:error, {:http_status, status, _body} = reason})
       when status in @retryable_statuses,
       do: {:transient, reason}

  defp classify({:error, {:http_status, status, _body, %{retry_after_ms: _}} = reason})
       when status in @retryable_statuses,
       do: {:transient, reason}

  defp classify({:error, reason}), do: {:permanent, reason}
  defp classify(other), do: {:permanent, {:unexpected_post_result, other}}

  defp retry_after({:error, {:http_status, status, _body, %{retry_after_ms: ms}}})
       when status in [429, 503],
       do: ms

  defp retry_after(_result), do: nil

  defp retry_delay(config, n, retry_after_ms) do
    retry_delay_ms(n, config.backoff_ms, config.max_backoff_ms, retry_after_ms, :rand.uniform())
  end

  @doc false
  # The wait after failed attempt `n` (1-based): `base * 2^(n-1)` capped at `cap`, with
  # jitter (50-100% of the step, `jitter` in [0, 1]), raised to a server Retry-After
  # when that asks for longer, and never longer than `cap`.
  @spec retry_delay_ms(
          pos_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer() | nil,
          float()
        ) :: non_neg_integer()
  def retry_delay_ms(n, base, cap, retry_after_ms, jitter) do
    step = min(base * Integer.pow(2, n - 1), cap)
    jittered = trunc(step * (0.5 + jitter / 2))
    min(cap, max(jittered, retry_after_ms || 0))
  end

  # -- outcomes (runs in the server) ----------------------------------------

  defp settle(state, results) do
    {kind, _} = elem(state.worker, 3)

    Enum.reduce(results, state, fn
      {item, :ok}, acc ->
        mark(acc.config, item, :mark_exported)
        callback(acc.config.on_event, Map.get(item, :event))
        acc

      {item, {:transient, reason}}, acc ->
        mark(acc.config, item, :mark_failed)
        if kind == :send, do: report(acc.config, reason)
        retain(acc, item)

      {item, {:permanent, reason}}, acc ->
        mark(acc.config, item, :mark_failed)
        report(acc.config, reason)
        acc
    end)
  end

  defp fail_without_send(state, item, reason, retain?) do
    mark(state.config, item, :mark_failed)
    report(state.config, reason)
    if retain?, do: retain(state, item), else: state
  end

  defp retain(state, item) do
    %{state | retained: Enum.take([item | state.retained], state.config.max_retained_failed)}
  end

  defp mark(%{sequence: nil}, _item, _fun), do: :ok

  defp mark(config, item, fun) do
    case Map.get(item, :slot) do
      %{epoch: _, number: _} = slot -> apply(Sequence, fun, [config.sequence, slot])
      _ -> :ok
    end
  catch
    _, _ -> :ok
  end

  defp report(config, reason) do
    callback(config.on_error, reason)

    ErrorLog.warn(
      {__MODULE__, config.source_handle},
      "trace export failed: " <> Http.format_error(reason),
      config.log_interval_ms
    )
  end

  defp callback(nil, _arg), do: :ok

  defp callback(fun, arg) do
    fun.(arg)
    :ok
  catch
    _, _ -> :ok
  end

  defp schedule_retry(%{config: %{retry_interval_ms: 0}} = state), do: state

  defp schedule_retry(state) do
    %{state | timer: Process.send_after(self(), :retry_failed, state.config.retry_interval_ms)}
  end

  # -- options --------------------------------------------------------------

  defp config!(opts) do
    {endpoint, handle} = target!(opts)

    %{
      endpoint: endpoint,
      source_handle: handle,
      sequence: Keyword.get(opts, :sequence),
      resource_attributes: Keyword.get(opts, :resource_attributes, []),
      on_error: Keyword.get(opts, :on_error),
      on_event: Keyword.get(opts, :on_event),
      max_queue: positive!(opts, :max_queue, 2048),
      batch_size: positive!(opts, :batch_size, 64),
      max_attempts: positive!(opts, :max_attempts, 5),
      backoff_ms: non_negative!(opts, :backoff_ms, 100),
      max_backoff_ms: non_negative!(opts, :max_backoff_ms, 5_000),
      retry_interval_ms: non_negative!(opts, :retry_interval_ms, 30_000),
      max_retained_failed: non_negative!(opts, :max_retained_failed, 1024),
      log_interval_ms: non_negative!(opts, :log_interval_ms, 10_000),
      sleep: Keyword.get(opts, :sleep, &Process.sleep/1),
      post: Keyword.get(opts, :post, &Exporter.export_traces/4)
    }
  end

  defp target!(opts) do
    case Keyword.get(opts, :otlp) do
      %{endpoint: endpoint, source_handle: handle} when is_binary(endpoint) and is_binary(handle) ->
        {endpoint, handle}

      _ ->
        endpoint = Keyword.get(opts, :endpoint)
        handle = Keyword.get(opts, :source_handle)

        if is_binary(endpoint) and is_binary(handle) do
          {endpoint, handle}
        else
          raise ArgumentError, ":endpoint and :source_handle (or :otlp) are required"
        end
    end
  end

  defp positive!(opts, key, default) do
    case Keyword.get(opts, key, default) do
      n when is_integer(n) and n > 0 -> n
      other -> raise ArgumentError, "#{key} must be a positive integer, got: #{inspect(other)}"
    end
  end

  defp non_negative!(opts, key, default) do
    case Keyword.get(opts, key, default) do
      n when is_integer(n) and n >= 0 -> n
      other -> raise ArgumentError, "#{key} must be a non-negative integer, got: #{inspect(other)}"
    end
  end
end
