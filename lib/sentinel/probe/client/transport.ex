defmodule Sentinel.Probe.SDK.Client.Transport do
  @moduledoc """
  Connect `application/proto` (binary) transport over Finch — NOT Connect-JSON.

  Elixir analog of `sdk/go/client/transport.go`. The Go analog wraps
  `connectrpc.com/connect`, which is built on stdlib `net/http`; on BEAM the
  equivalent is Finch (a process-based HTTP client over Mint) and a hand-rolled
  Connect unary envelope, since there is no production Connect client for
  Elixir. `:protobuf` encodes/decodes natively; `:jason` is used ONLY for the
  Connect error envelope (always JSON, even over the binary transport).

  ## Connect unary wire format (`application/proto`)

  Unary is the simplest Connect stream: request and response bodies are the raw
  protobuf message, with `Content-Type: application/proto` and
  `Connect-Protocol-Version: 1`. There is no 5-byte length-prefixed framing —
  that is for streaming. On a non-2xx the response body is the JSON error
  envelope `{"code", "message", "details"}`.

  ## Protocol lock-in

  Locking to Connect unary vs gRPC is a risk flagged against #22. HTTP/2 is
  deliberately NOT forced: whoever lands the decision endpoint decides, and a
  host can pass `:finch` options (including HTTP/2) through `decide/4`.
  """

  alias Sentinel.Probe.V1.{DecideRequest, DecideResponse}

  @service "sentinel.probe.v1.SentinelDecisionService"
  @method "Decide"

  @doc false
  def service, do: @service
  def method, do: @method

  @doc "The Connect unary path for the Decide RPC."
  @spec path :: String.t()
  def path, do: "/#{@service}/#{@method}"

  @typedoc """
  A transport failure rendered for the audit record.

    * `:deadline` — the host's own latency budget ran out (Finch timeout).
      Classified by the gate as `context-deadline-exceeded`.
    * `:cancelled` — the request was cancelled. Classified as `context-canceled`.
    * `:connect` — the endpoint returned a Connect error envelope. `code` is the
      Connect code atom (e.g. `:unavailable`). Classified as `connect-<code>`.
    * `:transport` — a generic transport failure. Classified as `transport-error`.
  """
  @type error_kind :: :deadline | :cancelled | :connect | :transport

  @type error :: %__MODULE__.Error{
          kind: error_kind(),
          code: atom() | nil,
          message: String.t()
        }

  defmodule Error do
    @moduledoc false
    defstruct [:kind, :code, :message]
  end

  @doc "Encodes a `DecideRequest` to its raw protobuf bytes (the request body)."
  @spec encode_request(DecideRequest.t()) :: binary()
  def encode_request(%DecideRequest{} = request) do
    IO.iodata_to_binary(Protobuf.encode(request))
  end

  @doc "Decodes raw protobuf bytes (a success response body) into a `DecideResponse`."
  @spec decode_response(binary()) :: {:ok, DecideResponse.t()} | {:error, error()}
  def decode_response(body) when is_binary(body) do
    {:ok, Protobuf.decode(body, DecideResponse)}
  rescue
    e -> {:error, %Error{kind: :transport, message: "decode failed: #{Exception.message(e)}"}}
  end

  @doc "Parses a Connect error envelope (JSON) into a transport error."
  @spec parse_error(non_neg_integer(), binary()) :: error()
  def parse_error(status, body) when is_binary(body) do
    case Jason.decode(body) do
      {:ok, %{"code" => code_str, "message" => message}}
      when is_binary(code_str) and is_binary(message) ->
        %Error{kind: :connect, code: String.to_atom(code_str), message: message}

      {:ok, %{"message" => message}} when is_binary(message) ->
        %Error{kind: :connect, code: nil, message: message}

      _ ->
        %Error{
          kind: :transport,
          message: "HTTP #{status}: unparseable Connect error envelope"
        }
    end
  end

  @doc """
  Issues the Decide RPC over Connect `application/proto`.

  `base_url` is the Sentinel decision endpoint base (e.g.
  `http://sentinel.local:7070`). `finch` is the `Finch` name the host started.
  `opts` are passed through to `Finch.request/3` (e.g. `receive_timeout`,
  `pool_timeout`) so a host can install its own latency budget as a Finch
  timeout — the transport never invents one.

  Returns `{:ok, DecideResponse.t()}` on a 2xx, or `{:error, Transport.Error.t()}`
  on any failure (local deadline, Connect error envelope, or generic transport
  error). The enforcement gate classifies the error kind for the audit record.
  """
  @spec decide(String.t(), DecideRequest.t(), atom(), keyword()) ::
          {:ok, DecideResponse.t()} | {:error, error()}
  def decide(base_url, %DecideRequest{} = request, finch, opts \\ []) do
    url = url(base_url)
    headers = [{"content-type", "application/proto"}, {"connect-protocol-version", "1"}]
    body = encode_request(request)

    request = Finch.build(:post, url, headers, body)

    case Finch.request(request, finch, opts) do
      {:ok, %Finch.Response{status: status, body: resp_body}} when status in 200..299 ->
        decode_response(resp_body)

      {:ok, %Finch.Response{status: status, body: resp_body}} ->
        {:error, parse_error(status, resp_body)}

      {:error, %Mint.TransportError{reason: :timeout}} ->
        {:error, %Error{kind: :deadline, message: "finch receive timeout"}}

      {:error, %Mint.TransportError{reason: :closed} = e} ->
        {:error, %Error{kind: :cancelled, message: Exception.message(e)}}

      {:error, %_{__exception__: true} = e} ->
        {:error, %Error{kind: :transport, message: Exception.message(e)}}

      {:error, reason} ->
        {:error, %Error{kind: :transport, message: inspect(reason)}}
    end
  end

  defp url(base_url) do
    String.trim_trailing(base_url, "/") <> path()
  end
end
