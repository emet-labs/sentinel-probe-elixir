defmodule Sentinel.Probe.SDK.Application do
  @moduledoc false
  # Supervises the FilterStore writers, so a writer that dies is reported,
  # never silently gone. Each writer is locally registered under a name derived
  # from its store key, which makes starting one atomic per key.
  use Application

  alias Sentinel.Probe.SDK.Client.FilterStore

  @impl true
  def start(_type, _args) do
    children = [
      {DynamicSupervisor, strategy: :one_for_one, name: FilterStore.Supervisor}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Sentinel.Probe.SDK.Supervisor)
  end
end
