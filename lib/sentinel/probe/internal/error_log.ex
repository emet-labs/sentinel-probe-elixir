defmodule Sentinel.Probe.SDK.Internal.ErrorLog do
  @moduledoc false
  # A rate-limited warning. The span-end path can fail once per span while Sentinel is down, so the
  # log line is emitted at most once per interval and counts what it suppressed. State is a pair of
  # atomics kept in :persistent_term per key, so the stateless span processor can use it too.

  require Logger

  @default_interval_ms 10_000

  @spec warn(term(), String.t(), non_neg_integer()) :: :ok
  def warn(key, message, interval_ms \\ @default_interval_ms) do
    ref = counters(key)
    now = System.system_time(:millisecond)
    last = :atomics.get(ref, 1)

    if last == 0 or now - last >= interval_ms do
      suppressed = :atomics.exchange(ref, 2, 0)
      :atomics.put(ref, 1, now)
      suffix = if suppressed > 0, do: " (#{suppressed} similar suppressed)", else: ""
      Logger.warning("sentinel probe: " <> message <> suffix)
    else
      :atomics.add(ref, 2, 1)
    end

    :ok
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end

  defp counters(key) do
    pt_key = {__MODULE__, key}

    case :persistent_term.get(pt_key, nil) do
      nil ->
        ref = :atomics.new(2, signed: false)
        :persistent_term.put(pt_key, ref)
        ref

      ref ->
        ref
    end
  end
end
