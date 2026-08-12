defmodule Sentinel.Probe.SDK.MockDecider do
  @moduledoc false
  defstruct response: nil, error: nil

  alias Sentinel.Probe.V1.{DecideRequest, DecideResponse}

  def new(opts \\ []),
    do: %__MODULE__{response: Keyword.get(opts, :response), error: Keyword.get(opts, :error)}

  def decide(%__MODULE__{} = mock, %DecideRequest{} = request) do
    Process.put({__MODULE__, :calls}, Process.get({__MODULE__, :calls}, 0) + 1)
    Process.put({__MODULE__, :last_request}, request)

    cond do
      mock.error != nil -> {:error, mock.error}
      mock.response != nil -> {:ok, mock.response}
      true -> {:ok, %DecideResponse{request_id: request.request_id}}
    end
  end

  def call_count, do: Process.get({__MODULE__, :calls}, 0)
  def last_request, do: Process.get({__MODULE__, :last_request})

  def reset! do
    Process.delete({__MODULE__, :calls})
    Process.delete({__MODULE__, :last_request})
  end
end
