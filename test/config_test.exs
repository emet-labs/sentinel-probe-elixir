defmodule Sentinel.Probe.SDK.ConfigTest do
  use ExUnit.Case, async: true
  alias Sentinel.Probe.SDK.Config.SourceTier

  test "load parses a valid JSON config" do
    json = ~s({"source.a": {"tier": "ANCHOR"}, "source.b": {"tier": "CONTRIBUTING"}})
    assert {:ok, config} = SourceTier.load(json)
    assert config["source.a"].tier == "ANCHOR"
    assert config["source.b"].tier == "CONTRIBUTING"
  end

  test "load rejects invalid JSON" do
    assert {:error, msg} = SourceTier.load("{bad")
    assert String.contains?(msg, "invalid JSON")
  end

  test "load rejects non-object JSON" do
    assert {:error, msg} = SourceTier.load("[1,2]")
    assert String.contains?(msg, "must be an object")
  end

  test "parse rejects non-object input" do
    assert {:error, _} = SourceTier.parse([{"a", 1}])
  end

  test "parse rejects entry that is not an object" do
    assert {:error, msg} = SourceTier.parse(%{"a" => 1})
    assert String.contains?(msg, "must be an object")
  end

  test "parse rejects missing tier" do
    assert {:error, msg} = SourceTier.parse(%{"a" => %{"other" => 1}})
    assert String.contains?(msg, "has no tier")
  end

  test "parse rejects non-string tier" do
    assert {:error, msg} = SourceTier.parse(%{"a" => %{"tier" => 1}})
    assert String.contains?(msg, "must be a string")
  end

  test "parse rejects unknown tier" do
    assert {:error, msg} = SourceTier.parse(%{"a" => %{"tier" => "BOGUS"}})
    assert String.contains?(msg, "unknown tier")
  end

  test "parse preserves extra keys" do
    assert {:ok, config} = SourceTier.parse(%{"a" => %{"tier" => "ANCHOR", "note" => "hello"}})
    assert config["a"].extra["note"] == "hello"
  end

  test "tier_for_handle resolves ANCHOR" do
    assert {:ok, :SOURCE_TIER_ANCHOR} =
             SourceTier.tier_for_handle(%{"a" => %{tier: "ANCHOR", extra: %{}}}, "a")
  end

  test "tier_for_handle resolves CONTRIBUTING" do
    assert {:ok, :SOURCE_TIER_CONTRIBUTING} =
             SourceTier.tier_for_handle(%{"b" => %{tier: "CONTRIBUTING", extra: %{}}}, "b")
  end

  test "tier_for_handle rejects undeclared handle" do
    assert {:error, msg} = SourceTier.tier_for_handle(%{}, "missing")
    assert String.contains?(msg, "no entry")
  end

  test "tier_for_handle rejects unknown tier" do
    assert {:error, msg} = SourceTier.tier_for_handle(%{"a" => %{tier: "BOGUS", extra: %{}}}, "a")
    assert String.contains?(msg, "unknown tier")
  end
end
