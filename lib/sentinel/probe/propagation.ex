defmodule Sentinel.Probe.SDK.Propagation do
  @moduledoc """
  Carries the parent Event id across a process boundary.

  A producer that is a Probe puts its Event's id in the header
  `x-sentinel-parent-event-id` on the HTTP request or Kafka record it sends. The
  consumer's span then records it as `sentinel.parent.event.id`, which is the
  causal edge. The W3C `traceparent` carries a span id, never an Event id, so it
  cannot stand in for this header.

  `extract_parent_event_id/1` falls back to the `tracestate` entry
  `sentinel=<eventId>` when the header is absent.

  Headers are either a list of `{name, value}` tuples (Plug request headers,
  Kafka record headers, Req/Finch) or a map of name to value. Names compare
  case-insensitively. Both functions return the same shape they were given.
  """

  @header "x-sentinel-parent-event-id"
  @tracestate "tracestate"
  @tracestate_key "sentinel"

  @type headers :: [{String.t(), String.t()}] | %{optional(String.t()) => String.t()}

  @doc "The header name, `#{@header}`."
  @spec header_name :: String.t()
  def header_name, do: @header

  @doc """
  Sets `x-sentinel-parent-event-id` to `event_id`, replacing any existing value.

  A blank or non-binary `event_id` leaves the headers unchanged: sending an empty
  parent id would be worse than sending none.
  """
  @spec inject_parent_event_id(headers() | nil, String.t() | nil) :: headers()
  def inject_parent_event_id(nil, event_id), do: inject_parent_event_id([], event_id)

  def inject_parent_event_id(headers, event_id) when is_map(headers) do
    if usable?(event_id) do
      headers
      |> Map.reject(fn {name, _} -> same_name?(name, @header) end)
      |> Map.put(@header, event_id)
    else
      headers
    end
  end

  def inject_parent_event_id(headers, event_id) when is_list(headers) do
    if usable?(event_id) do
      Enum.reject(headers, fn {name, _} -> same_name?(name, @header) end) ++
        [{@header, event_id}]
    else
      headers
    end
  end

  @doc """
  Reads the parent Event id from `x-sentinel-parent-event-id`, else from the
  `tracestate` entry `sentinel=<eventId>`. Returns `nil` when neither carries a
  non-blank id. Header values may be binaries or charlists.
  """
  @spec extract_parent_event_id(headers() | nil) :: String.t() | nil
  def extract_parent_event_id(nil), do: nil

  def extract_parent_event_id(headers) when is_map(headers),
    do: headers |> Map.to_list() |> extract_parent_event_id()

  def extract_parent_event_id(headers) when is_list(headers) do
    pairs = for {name, value} <- headers, is_binary(name) or is_list(name), do: {name, value}

    case first_value(pairs, @header) do
      nil -> pairs |> first_value(@tracestate) |> from_tracestate()
      id -> id
    end
  end

  defp first_value(pairs, wanted) do
    Enum.find_value(pairs, fn {name, value} ->
      with true <- same_name?(name, wanted),
           text when is_binary(text) <- text(value),
           trimmed = String.trim(text),
           true <- trimmed != "" do
        trimmed
      else
        _ -> nil
      end
    end)
  end

  defp from_tracestate(nil), do: nil

  defp from_tracestate(state) do
    state
    |> String.split(",")
    |> Enum.find_value(fn member ->
      case member |> String.trim() |> String.split("=", parts: 2) do
        [@tracestate_key, id] -> if String.trim(id) == "", do: nil, else: String.trim(id)
        _ -> nil
      end
    end)
  end

  defp usable?(event_id), do: is_binary(event_id) and String.trim(event_id) != ""

  defp text(value) when is_binary(value), do: value

  defp text(value) when is_list(value) do
    case :unicode.characters_to_binary(value) do
      text when is_binary(text) -> text
      _ -> nil
    end
  end

  defp text(_), do: nil

  defp same_name?(name, wanted), do: String.downcase(to_string(name)) == wanted
end
