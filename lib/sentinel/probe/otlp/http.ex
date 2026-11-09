defmodule Sentinel.Probe.SDK.Otlp.Http do
  @moduledoc """
  Loopback HTTP/1.1 for OTLP/HTTP protobuf.

  The Probe posts identity protobuf and accepts identity or gzip on the log
  receiver. This is not the Connect client.
  """

  @protobuf "application/x-protobuf"
  @max_header 65_536
  @max_body 8_388_608
  @timeout 5_000

  @doc """
  POSTs `body` to `endpoint <> path`.

  `endpoint` is an origin such as `http://127.0.0.1:8080`. A 2xx response is
  `{:ok, body}`.
  """
  @spec post(String.t(), String.t(), [{String.t(), String.t()}], binary(), keyword()) ::
          {:ok, binary()} | {:error, term()}
  def post(endpoint, path, headers, body, opts \\ []) when is_binary(body) do
    uri = URI.parse(endpoint)
    host = uri.host || "127.0.0.1"
    port = uri.port || 80
    content_type = Keyword.get(opts, :content_type, @protobuf)

    with {:ok, address} <- address(host),
         {:ok, socket} <-
           :gen_tcp.connect(address, port, [:binary, active: false, packet: :raw], @timeout),
         :ok <- :gen_tcp.send(socket, request(host, port, path, content_type, headers, body)) do
      result = read_response(socket)
      _ = :gen_tcp.close(socket)
      result
    else
      {:error, reason} -> {:error, {:transport, reason}}
    end
  end

  @doc "Reads one HTTP/1.1 message from `socket`. The body is exactly Content-Length bytes."
  @spec read_message(port(), timeout()) :: {:ok, String.t(), binary()} | {:error, term()}
  def read_message(socket, timeout \\ @timeout) do
    with {:ok, header, rest} <- read_headers(socket, <<>>, timeout, 0) do
      case content_length(header) do
        {:ok, length} when length <= @max_body ->
          case take_body(socket, rest, length, timeout) do
            {:ok, body} -> {:ok, header, body}
            {:error, reason} -> {:error, reason}
          end

        {:ok, _} ->
          {:error, :body_too_large}

        :error ->
          {:error, :missing_content_length}
      end
    end
  end

  @doc "Header map with lowercase names."
  @spec header_map(String.t()) :: %{optional(String.t()) => String.t()}
  def header_map(header) when is_binary(header) do
    header
    |> String.split("\r\n")
    |> Enum.drop(1)
    |> Enum.reduce(%{}, fn line, acc ->
      case String.split(line, ":", parts: 2) do
        [name, value] -> Map.put(acc, String.downcase(String.trim(name)), String.trim(value))
        _ -> acc
      end
    end)
  end

  @doc "Status code from a response header, or an error."
  @spec status_code(String.t()) :: {:ok, non_neg_integer()} | :error
  def status_code(header) do
    case header |> String.split("\r\n") |> List.first("") |> String.split(" ") do
      ["HTTP/1." <> _, code | _] ->
        case Integer.parse(code) do
          {status, ""} -> {:ok, status}
          _ -> :error
        end

      _ ->
        :error
    end
  end

  @spec request_line(String.t()) :: {String.t(), String.t()} | :error
  def request_line(header) do
    case header |> String.split("\r\n") |> List.first("") |> String.split(" ") do
      [method, path | _] -> {method, path}
      _ -> :error
    end
  end

  defp request(host, port, path, content_type, headers, body) do
    extra =
      Enum.map(headers, fn {name, value} ->
        [name, ": ", value, "\r\n"]
      end)

    [
      "POST ",
      path,
      " HTTP/1.1\r\nHost: ",
      host,
      ":",
      Integer.to_string(port),
      "\r\nContent-Type: ",
      content_type,
      "\r\nContent-Length: ",
      Integer.to_string(byte_size(body)),
      "\r\nConnection: close\r\n",
      extra,
      "\r\n",
      body
    ]
  end

  defp read_response(socket) do
    case read_message(socket) do
      {:ok, header, body} ->
        case status_code(header) do
          {:ok, status} when status in 200..299 -> {:ok, body}
          {:ok, status} -> {:error, {:http_status, status, body}}
          :error -> {:error, :bad_http_status}
        end

      {:error, reason} ->
        {:error, {:transport, reason}}
    end
  end

  defp read_headers(_socket, _buffer, _timeout, seen) when seen > @max_header do
    {:error, :header_too_large}
  end

  defp read_headers(socket, buffer, timeout, seen) do
    case :binary.match(buffer, "\r\n\r\n") do
      {index, _} ->
        header = binary_part(buffer, 0, index)
        rest = binary_part(buffer, index + 4, byte_size(buffer) - index - 4)
        {:ok, header, rest}

      :nomatch ->
        case :gen_tcp.recv(socket, 0, timeout) do
          {:ok, chunk} -> read_headers(socket, buffer <> chunk, timeout, seen + byte_size(chunk))
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp take_body(_socket, rest, length, _timeout) when byte_size(rest) >= length do
    {:ok, binary_part(rest, 0, length)}
  end

  defp take_body(socket, rest, length, timeout) do
    case :gen_tcp.recv(socket, length - byte_size(rest), timeout) do
      {:ok, chunk} -> {:ok, rest <> chunk}
      {:error, reason} -> {:error, reason}
    end
  end

  defp content_length(header) do
    case Map.fetch(header_map(header), "content-length") do
      {:ok, value} ->
        case Integer.parse(value) do
          {length, ""} when length >= 0 -> {:ok, length}
          _ -> :error
        end

      :error ->
        :error
    end
  end

  defp address(host) do
    charlist = String.to_charlist(host)

    case :inet.parse_address(charlist) do
      {:ok, addr} -> {:ok, addr}
      {:error, _} -> :inet.getaddr(charlist, :inet)
    end
  end
end
