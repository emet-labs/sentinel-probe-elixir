defmodule Sentinel.Probe.SDK.Config.SourceTier do
  @moduledoc """
  Reads a Probe's source tier from deployment configuration (ADR-0022). Elixir
  analog of `sdk/go/config/sourcetier.go` (`sdk/typescript/src/config/`).

  Tier is NEVER hard-coded. There is deliberately no list of anchor handles
  anywhere in this SDK: the deployment declares tiers, the Probe reads them at
  init and looks up its own `source_handle`. An undeclared handle is an error,
  never a silent default — defaulting would quietly demote or promote a source's
  evidentiary weight.

  JSON decoding stays out of the SDK for the non-JSON path: `parse/1` accepts an
  already-decoded `map`, so a host can use whatever parser it likes (YAML etc.) —
  the same reason the reference's `loadSourceTierConfig` takes `unknown`.
  `load/1` is the JSON convenience entry point.
  """

  alias Sentinel.Model.V1.SourceTier, as: ProtoSourceTier

  @tier_anchor "ANCHOR"
  @tier_contributing "CONTRIBUTING"

  @type source_entry :: %{
          tier: String.t(),
          extra: %{optional(String.t()) => term()}
        }

  @type t :: %{optional(String.t()) => source_entry()}

  @doc "The deployment-facing tier spellings (not the proto constant names)."
  @spec tier_anchor :: String.t()
  def tier_anchor, do: @tier_anchor
  @spec tier_contributing :: String.t()
  def tier_contributing, do: @tier_contributing

  @doc """
  Parses JSON configuration bytes.

  Returns `{:ok, config}` or `{:error, reason}`. The error reason is a
  descriptive string carrying the offending handle/value.
  """
  @spec load(binary()) :: {:ok, t()} | {:error, String.t()}
  def load(raw) when is_binary(raw) do
    case Jason.decode(raw) do
      {:ok, %{} = document} ->
        parse(document)

      {:ok, other} ->
        {:error, "source-tier: config must be an object, got #{inspect(other)}"}

      {:error, error} ->
        {:error, "source-tier: invalid JSON: #{Exception.message(error)}"}
    end
  end

  @doc """
  Validates configuration that has already been decoded.

  Preserves every key besides `:tier` under `extra`, mirroring the reference's
  zod `.passthrough()`: a deployment may carry its own annotations alongside the
  tier, and the SDK is not the right place to police them.
  """
  @spec parse(map()) :: {:ok, t()} | {:error, String.t()}
  def parse(%{} = raw) when not is_struct(raw) do
    Enum.reduce_while(raw, {:ok, %{}}, fn {handle, value}, {:ok, acc} ->
      case validate_entry(handle, value) do
        {:ok, entry} -> {:cont, {:ok, Map.put(acc, handle, entry)}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  def parse(other) do
    {:error, "source-tier: config must be an object, got #{inspect(other)}"}
  end

  defp validate_entry(handle, value) when not is_map(value) do
    {:error, "source-tier: entry for #{inspect(handle)} must be an object, got #{inspect(value)}"}
  end

  defp validate_entry(handle, entry) do
    # Structs are maps with __struct__; reject them like a plain-object check.
    if is_struct(entry) do
      {:error, "source-tier: entry for #{inspect(handle)} must be an object, got #{inspect(entry)}"}
    else
      case Map.fetch(entry, "tier") do
        :error ->
          {:error, "source-tier: entry for #{inspect(handle)} has no tier"}

        {:ok, tier} when not is_binary(tier) ->
          {:error,
           "source-tier: tier for #{inspect(handle)} must be a string, got #{inspect(tier)}"}

        {:ok, tier} when tier in [@tier_anchor, @tier_contributing] ->
          extra = Map.drop(entry, ["tier"])
          {:ok, %{tier: tier, extra: extra}}

        {:ok, tier} ->
          {:error,
           "source-tier: unknown tier #{inspect(tier)} for #{inspect(handle)}, want " <>
             inspect(@tier_anchor) <> " or " <> inspect(@tier_contributing)}
      end
    end
  end

  @doc """
  Resolves the proto `SourceTier` for a `source_handle`.

  An undeclared handle is an error. It is never `:SOURCE_TIER_UNSPECIFIED` and
  never a default: a source whose tier nobody declared has no business claiming
  one.
  """
  @spec tier_for_handle(t(), String.t()) :: {:ok, ProtoSourceTier.t()} | {:error, String.t()}
  def tier_for_handle(config, source_handle) when is_map(config) do
    case Map.fetch(config, source_handle) do
      :error ->
        {:error, "source-tier: no entry for source_handle #{inspect(source_handle)}"}

      {:ok, %{tier: @tier_anchor}} ->
        {:ok, :SOURCE_TIER_ANCHOR}

      {:ok, %{tier: @tier_contributing}} ->
        {:ok, :SOURCE_TIER_CONTRIBUTING}

      {:ok, %{tier: other}} ->
        # Unreachable via the constructors above, which reject unknown tiers, but a
        # caller can build a config literal by hand.
        {:error,
         "source-tier: unknown tier #{inspect(other)} for source_handle #{inspect(source_handle)}"}
    end
  end
end
