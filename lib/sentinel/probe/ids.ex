defmodule Sentinel.Probe.SDK.IDs do
  @moduledoc """
  Generates the per-call identifiers a Probe stamps into decision requests.
  Elixir analog of `sdk/go/ids/ids.go`, which uses `github.com/google/uuid`.

  UUID v4 via the `:uuid` hex package — a deliberate convenience choice over
  rolling the RFC 4122 bit-twiddling by hand.
  """

  @doc "Returns a fresh request ID (UUID v4)."
  @spec generate_request_id :: String.t()
  def generate_request_id, do: UUID.uuid4()

  @doc """
  Returns a fresh idempotency key (UUID v4).

  A distinct function from `generate_request_id/0` even though the
  implementation is identical: the two identify different things. A retry of the
  same logical decision reuses the idempotency key while taking a new request
  ID, so collapsing them would make retries indistinguishable from fresh asks at
  the receiver.
  """
  @spec generate_idempotency_key :: String.t()
  def generate_idempotency_key, do: UUID.uuid4()
end
