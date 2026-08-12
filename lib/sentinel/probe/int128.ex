defmodule Sentinel.Probe.SDK.Int128 do
  @moduledoc """
  Encodes and decodes `sentinel.model.v1.Int128`, the 128-bit occurrence-time
  representation. Elixir analog of `sdk/go/int128/int128.go`.

  Elixir integers are arbitrary-precision natively, so a plain `integer()` is the
  canonical representation — the idiomatic equivalent of Go's `math/big.Int` and
  TypeScript's `bigint`, and what makes the round-trip meaningful. `from_int64/1`
  covers the common nanosecond case without any allocation.

  ## Encode direction (the trap)

  In `common.proto`, `low` is `fixed64` (UNSIGNED) and `high` is `sfixed64`
  (SIGNED), and the value is `high * 2^64 + low`:

    * encode: `low = value band 2^64-1` (as unsigned), `high = value bsr 64`
      (ARITHMETIC shift — Elixir's `bsr` sign-extends negatives);
    * decode: `value = high * 2^64 + low`, with NO sign correction — `high` is
      already signed.

  Worked check for `value = -1`: `high = -1`, `low = 0xFFFFFFFFFFFFFFFF`, and
  `-1 * 2^64 + (2^64 - 1) = -1`. Applying a second sign correction on decode is a
  real bug; it is not applied here.
  """

  import Bitwise

  # 2^64, the weight of the high word.
  @two_pow_64 0x10000000000000000
  # 2^64 - 1, the unsigned low-word mask.
  @low_mask 0xFFFFFFFFFFFFFFFF

  alias Sentinel.Model.V1.Int128

  @doc """
  Decodes an `Int128` into its integer value: `high * 2^64 + low`.

  A `nil` Int128 decodes to `0`, matching the generated getters' nil-safety.
  """
  @spec to_int(Int128.t() | nil) :: integer()
  def to_int(nil), do: 0

  def to_int(%Int128{high: high, low: low}) do
    high * @two_pow_64 + low
  end

  @doc """
  Encodes an integer value into an `Int128`.

  Values outside the signed 128-bit range are truncated to their low 128 bits,
  the same modular behaviour the proto's fixed words have. `bsr/2` is arithmetic
  on negatives (sign-extends), so the high word carries the sign.
  """
  @spec from_int(integer()) :: Int128.t()
  def from_int(value) do
    %Int128{high: bsr(value, 64), low: band(value, @low_mask)}
  end

  @doc """
  Encodes a 64-bit signed integer without going through the general path.

  `high` is the sign extension (`0` or `-1`) and `low` is the two's-complement
  bit pattern as an unsigned word. Equivalent to `from_int/1` for the int64
  range, but documents the common nanosecond case explicitly (mirrors Go's
  `FromInt64`).
  """
  @spec from_int64(integer()) :: Int128.t()
  def from_int64(value) do
    %Int128{high: bsr(value, 63), low: band(value, @low_mask)}
  end

  @doc """
  Returns the given instant as nanoseconds since the Unix epoch.

  The instant is expressed as `{seconds, nanos}` — the exact analog of Go's
  `time.Unix(seconds, nanos)` — so `seconds` may be negative (before the epoch)
  and `nanos` carries full nanosecond precision (`0..999_999_999`). The value is
  `seconds * 1_000_000_000 + nanos`.

  Deliberately NOT `DateTime.to_unix(dt, :nanosecond)`: a `DateTime` stores only
  microsecond precision, which would silently drop sub-microsecond nanos and
  break the Int128 round-trip. The big form here is exact for every
  representable instant — the same reason Go avoids `time.Time.UnixNano()`,
  which is undefined outside 1678–2262.

  A `DateTime` is also accepted for convenience, converted via
  `to_unix(:nanosecond)` (microsecond-granular, exact for whole-second
  instants). A plain `integer` is treated as already-unix-nanoseconds (the form
  the `Emission.Processor` produces from the OTel monotonic start time).
  """
  @spec time_to_nanoseconds(
          {integer(), integer()}
          | DateTime.t()
          | integer()
        ) :: integer()
  def time_to_nanoseconds({seconds, nanos}), do: seconds * 1_000_000_000 + nanos

  def time_to_nanoseconds(%DateTime{} = dt) do
    DateTime.to_unix(dt, :nanosecond)
  end

  def time_to_nanoseconds(value) when is_integer(value), do: value
end
