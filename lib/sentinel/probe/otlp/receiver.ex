defmodule Sentinel.Probe.SDK.Otlp.Receiver do
  @moduledoc """
  OTLP/HTTP log receiver. It binds `127.0.0.1` on an ephemeral port unless
  told otherwise (`:ip`, `:port`); a containerised Probe binds `{0, 0, 0, 0}`
  and a fixed port so Sentinel can reach it. The receiver has no
  authentication: bind a non-loopback address only on a network you trust.

  `POST /v1/logs` decodes `Verdict`, `EventFilter`, and `DecideResponse`.
  The body is protobuf bytes. Identity and gzip are accepted. An unspecified
  decision action is an error and is not delivered to the gate. An empty
  export stores nothing.
  """

  use GenServer

  alias Sentinel.Model.V1.EventFilter
  alias Sentinel.Probe.SDK.Client.FilterStore
  alias Sentinel.Probe.SDK.Otlp.{Codec, Http}

  @timeout 5_000

  defstruct [
    :listen,
    :port,
    :acceptor,
    verdicts: [],
    filters: [],
    decisions: %{},
    waiters: %{},
    filter_store: nil
  ]

  @doc """
  Starts the receiver.

    * `:ip` — the bind address as an `:inet` tuple or an address string
      (`{127, 0, 0, 1}` by default; `{0, 0, 0, 0}` for a container).
    * `:port` — the listen port (`0`, an ephemeral port, by default).
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc "Stops the receiver and closes the socket."
  @spec stop(GenServer.server()) :: :ok
  def stop(server), do: GenServer.stop(server)

  @doc "The loopback port."
  @spec port(GenServer.server()) :: non_neg_integer()
  def port(server), do: GenServer.call(server, :port)

  @doc "Origin Sentinel should use as `SENTINEL_PROBE_LOG_URL`'s host."
  @spec origin(GenServer.server()) :: String.t()
  def origin(server), do: "http://127.0.0.1:#{port(server)}"

  @doc "Verdicts received so far, oldest first."
  @spec verdicts(GenServer.server()) :: [struct()]
  def verdicts(server), do: GenServer.call(server, :verdicts)

  @doc "Event Filters received so far, oldest first."
  @spec filters(GenServer.server()) :: [EventFilter.t()]
  def filters(server), do: GenServer.call(server, :filters)

  @doc """
  Returns the `DecideResponse` stored for `request_id`.

  `timeout_ms` of `0` or less does not wait: a response that is already stored
  is returned, and a missing one is `{:error, :timeout}`. A positive timeout
  waits only until that budget, then times out. The gate uses this instead of
  Connect.
  """
  @spec await_decision(GenServer.server(), String.t(), integer()) ::
          {:ok, struct()} | {:error, :timeout}
  def await_decision(server, request_id, timeout_ms)
      when is_binary(request_id) and is_integer(timeout_ms) do
    case GenServer.call(server, {:lookup_decision, request_id}) do
      {:ok, response} ->
        {:ok, response}

      :missing when timeout_ms <= 0 ->
        {:error, :timeout}

      :missing ->
        wait(server, request_id, timeout_ms)
    end
  end

  @doc "Replaces the filter store when an Event Filter log arrives."
  @spec attach_filter_store(GenServer.server(), FilterStore.t()) :: :ok
  def attach_filter_store(server, %FilterStore{} = store) do
    GenServer.call(server, {:attach_filter_store, store})
  end

  @impl true
  def init(opts) do
    with {:ok, ip} <- bind_ip(Keyword.get(opts, :ip, {127, 0, 0, 1})),
         {:ok, port} <- bind_port(Keyword.get(opts, :port, 0)),
         listen_opts = [:binary, packet: :raw, active: false, ip: ip, reuseaddr: true],
         {:ok, listen} <- :gen_tcp.listen(port, listen_opts) do
      {:ok, port} = :inet.port(listen)
      parent = self()

      acceptor =
        spawn_link(fn ->
          receive do
            :go -> accept_loop(listen, parent)
          end
        end)

      :ok = :gen_tcp.controlling_process(listen, acceptor)
      send(acceptor, :go)
      {:ok, %__MODULE__{listen: listen, port: port, acceptor: acceptor}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  defp bind_ip(ip) when is_tuple(ip) and tuple_size(ip) in [4, 8], do: {:ok, ip}

  defp bind_ip(host) when is_binary(host) do
    case :inet.parse_address(String.to_charlist(host)) do
      {:ok, ip} -> {:ok, ip}
      {:error, _} -> {:error, {:invalid_bind_ip, host}}
    end
  end

  defp bind_ip(other), do: {:error, {:invalid_bind_ip, other}}

  defp bind_port(port) when is_integer(port) and port in 0..65_535, do: {:ok, port}
  defp bind_port(other), do: {:error, {:invalid_bind_port, other}}

  @impl true
  def terminate(_reason, %__MODULE__{acceptor: acceptor}) do
    if is_pid(acceptor) and Process.alive?(acceptor), do: Process.exit(acceptor, :shutdown)
    :ok
  end

  @impl true
  def handle_call(:port, _from, state), do: {:reply, state.port, state}
  def handle_call(:verdicts, _from, state), do: {:reply, Enum.reverse(state.verdicts), state}
  def handle_call(:filters, _from, state), do: {:reply, Enum.reverse(state.filters), state}

  def handle_call({:lookup_decision, request_id}, _from, state) do
    {:reply, Map.fetch(state.decisions, request_id) |> normalize_lookup(), state}
  end

  def handle_call({:attach_filter_store, store}, _from, state) do
    state = %{state | filter_store: store}
    if state.filters != [], do: FilterStore.set(store, hd(state.filters))
    {:reply, :ok, state}
  end

  def handle_call({:ingest, bytes}, _from, state) do
    case Codec.decode_probe_logs(bytes) do
      {:ok, records} -> {:reply, :ok, apply_records(records, state)}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:notify, request_id, waiter, ref}, _from, state) do
    case Map.fetch(state.decisions, request_id) do
      {:ok, response} ->
        send(waiter, {:decision, ref, response})
        {:reply, :ok, state}

      :error ->
        waiters = Map.update(state.waiters, request_id, [{waiter, ref}], &[{waiter, ref} | &1])
        {:reply, :ok, %{state | waiters: waiters}}
    end
  end

  @impl true
  def handle_cast({:cancel_wait, ref}, state) do
    waiters =
      Map.new(state.waiters, fn {request_id, entries} ->
        {request_id, Enum.reject(entries, fn {_waiter, entry} -> entry == ref end)}
      end)

    {:noreply, %{state | waiters: waiters}}
  end

  defp normalize_lookup({:ok, response}), do: {:ok, response}
  defp normalize_lookup(:error), do: :missing

  defp apply_records(records, state) do
    Enum.reduce(records, state, fn
      {:verdict, verdict}, state ->
        %{state | verdicts: [verdict | state.verdicts]}

      {:event_filter, filter}, state ->
        if state.filter_store, do: FilterStore.set(state.filter_store, filter)
        %{state | filters: [filter | state.filters]}

      {:decide, response}, state ->
        deliver(state.waiters[response.request_id] || [], response)
        waiters = Map.delete(state.waiters, response.request_id)
        decisions = Map.put(state.decisions, response.request_id, response)
        %{state | decisions: decisions, waiters: waiters}
    end)
  end

  defp deliver(waiters, response) do
    Enum.each(waiters, fn {waiter, ref} -> send(waiter, {:decision, ref, response}) end)
  end

  defp wait(server, request_id, timeout_ms) do
    ref = make_ref()
    :ok = GenServer.call(server, {:notify, request_id, self(), ref})

    receive do
      {:decision, ^ref, response} -> {:ok, response}
    after
      timeout_ms ->
        GenServer.cast(server, {:cancel_wait, ref})

        case GenServer.call(server, {:lookup_decision, request_id}) do
          {:ok, response} -> {:ok, response}
          :missing -> {:error, :timeout}
        end
    end
  end

  defp accept_loop(listen, server) do
    case :gen_tcp.accept(listen) do
      {:ok, socket} ->
        handle_socket(socket, server)
        accept_loop(listen, server)

      {:error, :closed} ->
        :ok

      {:error, _} ->
        :ok
    end
  end

  defp handle_socket(socket, server) do
    response =
      case Http.read_message(socket, @timeout) do
        {:ok, header, body} -> dispatch(server, header, body)
        {:error, reason} -> http_response(400, inspect(reason))
      end

    _ = :gen_tcp.send(socket, response)
    _ = :gen_tcp.close(socket)
  end

  defp dispatch(server, header, body) do
    headers = Http.header_map(header)

    cond do
      Http.request_line(header) != {"POST", "/v1/logs"} ->
        http_response(404, "not found")

      not protobuf?(headers["content-type"]) ->
        http_response(415, "OTLP/HTTP requires application/x-protobuf")

      true ->
        case inflate(headers["content-encoding"], body) do
          {:ok, bytes} ->
            case GenServer.call(server, {:ingest, bytes}, @timeout) do
              :ok -> http_response(200, "")
              {:error, reason} -> http_response(400, inspect(reason))
            end

          {:error, reason} ->
            http_response(400, inspect(reason))
        end
    end
  end

  defp protobuf?(nil), do: false

  defp protobuf?(type) do
    type
    |> String.split(";")
    |> List.first()
    |> String.trim()
    |> String.downcase() ==
      "application/x-protobuf"
  end

  defp inflate(nil, body), do: {:ok, body}
  defp inflate("identity", body), do: {:ok, body}
  defp inflate("gzip", body), do: gunzip(body)
  defp inflate(other, _body), do: {:error, {:unsupported_encoding, other}}

  defp gunzip(body) do
    {:ok, :zlib.gunzip(body)}
  rescue
    error -> {:error, {:gzip, Exception.message(error)}}
  end

  defp http_response(status, body) do
    reason =
      case status do
        200 -> "OK"
        400 -> "Bad Request"
        404 -> "Not Found"
        415 -> "Unsupported Media Type"
        _ -> "Error"
      end

    [
      "HTTP/1.1 ",
      Integer.to_string(status),
      " ",
      reason,
      "\r\nContent-Length: ",
      Integer.to_string(byte_size(body)),
      "\r\nConnection: close\r\n\r\n",
      body
    ]
  end
end
