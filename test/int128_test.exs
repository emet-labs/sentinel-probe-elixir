defmodule Sentinel.Probe.SDK.Int128Test do
  use ExUnit.Case, async: true

  alias Sentinel.Model.V1.Int128, as: ProtoInt128
  alias Sentinel.Probe.SDK.Int128

  @cases [
    {"zero", 0},
    {"one", 1},
    {"fits in the low word", 1_700_000_000_123_456_789},
    {"max low word", 18_446_744_073_709_551_615},
    {"spans both words", 18_446_744_073_709_551_616},
    {"large positive", 170_141_183_460_469_231_731_687_303_715_884_105_727},
    {"negative one", -1},
    {"negative spanning both words", -18_446_744_073_709_551_617},
    {"large negative", -170_141_183_460_469_231_731_687_303_715_884_105_728}
  ]

  test "round-trip through Int128 is exact" do
    for {name, value} <- @cases do
      assert Int128.to_int(Int128.from_int(value)) == value, "#{name}: round-trip"
    end
  end

  test "encode word signedness: low is unsigned, high is signed" do
    positive = Int128.from_int(18_446_744_073_709_551_617)
    assert positive.high == 1
    assert positive.low == 1

    # The worked check from the package doc: -1 is high=-1, low=2^64-1, and
    # -1*2^64 + (2^64-1) = -1 with NO sign correction on decode.
    negative = Int128.from_int(-1)
    assert negative.high == -1
    assert negative.low == 18_446_744_073_709_551_615
    assert Int128.to_int(negative) == -1
  end

  test "from_int64 matches from_int for the int64 range" do
    for value <- [
          0,
          1,
          -1,
          9_223_372_036_854_775_807,
          -9_223_372_036_854_775_808,
          1_700_000_000_123_456_789,
          -1_700_000_000_123_456_789
        ] do
      fast = Int128.from_int64(value)
      slow = Int128.from_int(value)
      assert fast.high == slow.high
      assert fast.low == slow.low
      assert Int128.to_int(fast) == value
    end
  end

  test "to_int is nil-safe (decodes to zero)" do
    assert Int128.to_int(nil) == 0
    assert Int128.to_int(%ProtoInt128{}) == 0
  end

  test "time_to_nanoseconds mirrors Go's seconds*1e9 + nanos" do
    cases = [
      {{"epoch", {0, 0}}, 0},
      {{"whole seconds", {1_700_000_000, 0}}, 1_700_000_000_000_000_000},
      {{"seconds and nanos", {1_700_000_000, 123_456_789}}, 1_700_000_000_123_456_789},
      {{"before the epoch", {-1, 500_000_000}}, -500_000_000}
    ]

    for {{name, {seconds, nanos}}, want} <- cases do
      assert Int128.time_to_nanoseconds({seconds, nanos}) == want, "#{name}"
    end
  end

  test "time_to_nanoseconds for whole-second Dates is exact" do
    # year 3000 and year 1000 mirror the Go analog's beyond-int64 cases.
    assert Int128.time_to_nanoseconds(~U[3000-01-01 00:00:00Z]) == 32_503_680_000_000_000_000
    assert Int128.time_to_nanoseconds(~U[1000-01-01 00:00:00Z]) == -30_610_224_000_000_000_000
  end

  test "time_to_nanoseconds round-trips through Int128" do
    for moment <- [
          {1_700_000_000, 123_456_789},
          ~U[3000-01-01 00:00:00Z],
          ~U[1000-01-01 00:00:00Z]
        ] do
      nanos = Int128.time_to_nanoseconds(moment)
      assert Int128.to_int(Int128.from_int(nanos)) == nanos
    end
  end

  test "a value beyond 2^64 uses the high word" do
    nanos = Int128.time_to_nanoseconds(~U[3000-01-01 00:00:00Z])
    encoded = Int128.from_int(nanos)
    assert encoded.high != 0
    assert Int128.to_int(encoded) == nanos
  end
end
