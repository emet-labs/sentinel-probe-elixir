defmodule Sentinel.Probe.SDK.ConformanceFilterStoreTest do
  # The filter_store suite (manifest-v1.json, 1.4.0) through the production FilterStore.
  # Epochs are decimal strings parsed into unbounded integers, so the full unsigned 64-bit range
  # is exact. A held Filter is compared as a term: Elixir has no reference identity.
  use ExUnit.Case, async: true

  alias Sentinel.Model.V1.{EventFilter, SpecificationFilter}
  alias Sentinel.Probe.SDK.Client.FilterStore

  @root Path.expand("../testdata/probe-sdk-conformance", __DIR__)

  defp load(name), do: @root |> Path.join(name) |> File.read!() |> Jason.decode!()

  defp epoch(nil), do: nil
  defp epoch(value), do: String.to_integer(value)

  defp build(fixture) do
    %EventFilter{
      epoch: epoch(fixture["epoch"]),
      specifications: [%SpecificationFilter{specification_id: fixture["label"]}]
    }
  end

  test "filter_store vectors replay the production store" do
    manifest = load("manifest-v1.json")
    assert manifest["format_version"] == "1.4.0"
    assert [entry] = Enum.filter(manifest["suites"], &(&1["kind"] == "filter_store"))
    suite = load(entry["path"])
    assert suite["format_version"] == "1.2.0"
    assert suite["kind"] == "filter_store"
    assert suite["cases"] != []

    for vector <- suite["cases"] do
      applied = vector["initial"] && build(vector["initial"])
      store = FilterStore.new({:conformance_filter_store, vector["id"]}, applied)

      vector["steps"]
      |> Enum.with_index()
      |> Enum.reduce({applied, true}, fn {step, index}, {applied, track} ->
        at = "#{vector["id"]} step #{index}"

        case step["op"] do
          "should_refresh" ->
            assert FilterStore.should_refresh(store, epoch(step["epoch"])) == step["expected"], at
            {applied, track}

          "set" ->
            offered = build(step["filter"])
            replaced = FilterStore.set(store, offered)
            expected = step["expected"]

            {applied, track} =
              case Map.fetch(expected, "applied") do
                {:ok, want} ->
                  assert replaced == want, at
                  {if(replaced, do: offered, else: applied), track}

                # An equal-epoch set pins only the held content.
                :error ->
                  {applied, false}
              end

            held = FilterStore.get(store)
            if track, do: assert(held == applied, at)

            assert Enum.map(held.specifications, & &1.specification_id) == [
                     expected["held"]["label"]
                   ],
                   at

            assert held.epoch == epoch(expected["held"]["epoch"]), at
            assert FilterStore.epoch(store) == epoch(expected["held"]["epoch"]), at
            {applied, track}
        end
      end)

      FilterStore.delete(store)
    end
  end
end
