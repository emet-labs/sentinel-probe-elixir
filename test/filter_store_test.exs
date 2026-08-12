defmodule Sentinel.Probe.SDK.FilterStoreTest do
  use ExUnit.Case, async: true
  alias Sentinel.Model.V1.{EventFilter, SpecificationFilter}
  alias Sentinel.Probe.SDK.Client.FilterStore

  defp key(n), do: {:sentinel_test_filter, n, self()}

  defp new_store(n, initial \\ nil), do: FilterStore.new(key(n), initial)

  defp filter(epoch), do: %EventFilter{epoch: epoch, specifications: []}

  test "get returns nil before the first set" do
    store = new_store(:get_nil)
    assert FilterStore.get(store) == nil
  after
    FilterStore.delete(struct(FilterStore, key: key(:get_nil)))
  end

  test "set returns true on the first set" do
    store = new_store(:first)
    assert FilterStore.set(store, filter(5)) == true
    assert FilterStore.get(store) != nil
  after
    FilterStore.delete(struct(FilterStore, key: key(:first)))
  end

  test "set returns false when the epoch is unchanged" do
    store = new_store(:unchanged)
    assert FilterStore.set(store, filter(5)) == true
    assert FilterStore.set(store, filter(5)) == false
  after
    FilterStore.delete(struct(FilterStore, key: key(:unchanged)))
  end

  test "set returns true when the epoch changes" do
    store = new_store(:changed)
    assert FilterStore.set(store, filter(5)) == true
    assert FilterStore.set(store, filter(6)) == true
  after
    FilterStore.delete(struct(FilterStore, key: key(:changed)))
  end

  test "epoch compares by value, not by identity" do
    store = new_store(:by_value)
    f1 = %EventFilter{epoch: 7, specifications: [%SpecificationFilter{specification_id: "a"}]}
    f2 = %EventFilter{epoch: 7, specifications: [%SpecificationFilter{specification_id: "b"}]}
    assert FilterStore.set(store, f1) == true
    # Same epoch, different struct — must NOT report updated.
    assert FilterStore.set(store, f2) == false
    assert FilterStore.get(store).specifications == [%SpecificationFilter{specification_id: "a"}]
  after
    FilterStore.delete(struct(FilterStore, key: key(:by_value)))
  end

  test "epoch 0 is a legitimate epoch, not absent" do
    store = new_store(:zero)
    assert FilterStore.set(store, filter(0)) == true
    assert FilterStore.epoch(store) == 0
    refute FilterStore.epoch(store) == nil
  after
    FilterStore.delete(struct(FilterStore, key: key(:zero)))
  end

  test "epoch returns nil for no filter" do
    store = new_store(:nil_epoch)
    assert FilterStore.epoch(store) == nil
  after
    FilterStore.delete(struct(FilterStore, key: key(:nil_epoch)))
  end

  test "should_refresh: no filter held means refresh" do
    store = new_store(:refresh_no_filter)
    assert FilterStore.should_refresh(store, 1) == true
  after
    FilterStore.delete(struct(FilterStore, key: key(:refresh_no_filter)))
  end

  test "should_refresh: no filter held with nil epoch means refresh" do
    store = new_store(:refresh_nil)
    assert FilterStore.should_refresh(store, nil) == true
  after
    FilterStore.delete(struct(FilterStore, key: key(:refresh_nil)))
  end

  test "should_refresh: different epoch means refresh" do
    store = new_store(:refresh_diff)
    FilterStore.set(store, filter(5))
    assert FilterStore.should_refresh(store, 6) == true
  after
    FilterStore.delete(struct(FilterStore, key: key(:refresh_diff)))
  end

  test "should_refresh: same epoch means no refresh" do
    store = new_store(:refresh_same)
    FilterStore.set(store, filter(5))
    assert FilterStore.should_refresh(store, 5) == false
  after
    FilterStore.delete(struct(FilterStore, key: key(:refresh_same)))
  end

  test "both no-epoch filters still counts as update on first set" do
    store = new_store(:both_nil)
    f1 = %EventFilter{epoch: nil}
    f2 = %EventFilter{epoch: nil}
    assert FilterStore.set(store, f1) == true
    assert FilterStore.set(store, f2) == false
  after
    FilterStore.delete(struct(FilterStore, key: key(:both_nil)))
  end
end
