defmodule Sentinel.Probe.SDK.IDsTest do
  use ExUnit.Case, async: true

  alias Sentinel.Probe.SDK.IDs

  test "generate_request_id is a UUID v4" do
    assert String.match?(
             IDs.generate_request_id(),
             ~r/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/
           )
  end

  test "generate_idempotency_key is a UUID v4" do
    assert String.match?(
             IDs.generate_idempotency_key(),
             ~r/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/
           )
  end

  test "fresh ids are distinct" do
    ids = for _ <- 1..100, do: IDs.generate_request_id()
    assert length(Enum.uniq(ids)) == 100
  end

  test "request id and idempotency key are independent calls" do
    # Distinct functions even though the implementation is identical: a retry
    # reuses the idempotency key while taking a new request id.
    assert is_binary(IDs.generate_request_id())
    assert is_binary(IDs.generate_idempotency_key())
  end
end
