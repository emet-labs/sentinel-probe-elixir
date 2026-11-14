defmodule Sentinel.Probe.SDK.PartialSuccessTest do
  use ExUnit.Case, async: true

  alias Sentinel.Probe.SDK.Otlp.PartialSuccess

  test "an empty body rejects nothing" do
    assert PartialSuccess.rejected_spans(<<>>) == {:ok, 0}
  end

  test "rejected spans are read from partial_success" do
    assert PartialSuccess.rejected_spans(Base.decode16!("0a020802", case: :lower)) == {:ok, 2}
  end

  test "a duplicate message with zero rejected spans rejects nothing" do
    message = "append_events classified 1 deliveries as duplicate"
    inner = <<0x12, byte_size(message)>> <> message
    assert PartialSuccess.rejected_spans(<<0x0A, byte_size(inner)>> <> inner) == {:ok, 0}
  end

  test "a truncated body is undecodable" do
    assert {:error, _} = PartialSuccess.rejected_spans(Base.decode16!("0a0508", case: :lower))
  end

  test "a negative rejected count is undecodable" do
    # rejected_spans = -1: a ten-byte two's-complement varint.
    inner = <<0x08, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01>>
    assert {:error, _} = PartialSuccess.rejected_spans(<<0x0A, byte_size(inner)>> <> inner)
  end

  test "unknown fields are skipped" do
    inner = <<0x08, 0x03, 0x1A, 0x01, 0x00>>
    body = <<0x15, 0, 0, 0, 0, 0x0A, byte_size(inner)>> <> inner
    assert PartialSuccess.rejected_spans(body) == {:ok, 3}
  end
end
