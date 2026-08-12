defmodule Sentinel.Probe.SDK.OccurrenceTimeTest do
  use ExUnit.Case, async: true
  alias Sentinel.Probe.SDK.Emission.{Span, SpanToEvent}
  alias Sentinel.Probe.SDK.Int128

  test "uses unix clock domain and zero uncertainty" do
    ot = SpanToEvent.build_occurrence_time({1_700_000_000, 123_456_789})
    assert ot.clock_domain_id == "unix"
    assert ot.uncertainty_nanoseconds == 0
  end

  test "nanoseconds are exact for seconds+nanos" do
    ot = SpanToEvent.build_occurrence_time({1_700_000_000, 123_456_789})
    assert Int128.to_int(ot.nanoseconds) == 1_700_000_000_123_456_789
  end

  test "comes from start_time, not end_time" do
    event = SpanToEvent.span_to_event(%Span{name: "x", start_time: {1_700_000_000, 0}})
    assert Int128.to_int(event.occurrence_time.nanoseconds) == 1_700_000_000_000_000_000
  end

  test "survives beyond int64 nanoseconds (year 3000)" do
    ot = SpanToEvent.build_occurrence_time(~U[3000-01-01 00:00:00Z])
    assert ot.nanoseconds.high != 0
    assert Int128.to_int(ot.nanoseconds) == 32_503_680_000_000_000_000
  end

  test "before the epoch" do
    ot = SpanToEvent.build_occurrence_time({-1, 500_000_000})
    assert ot.nanoseconds.high == -1
    assert Int128.to_int(ot.nanoseconds) == -500_000_000
  end
end
