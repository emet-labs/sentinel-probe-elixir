defmodule Sentinel.Probe.SDK.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/emet-labs/sentinel-probe-elixir"

  def project do
    [
      app: :sentinel_probe_sdk,
      version: @version,
      elixir: "~> 1.16",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      elixirc_paths: elixirc_paths(Mix.env()),
      package: package(),
      description: description(),
      name: "Sentinel Probe SDK",
      source_url: @source_url
    ]
  end

  def application do
    [
      # Finch needs to be started for the transport, but the SDK itself is
      # library code; the host starts the OTel exporter (ADR-0002). extra_applications
      # are intentionally minimal.
      extra_applications: [:logger]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "gen", "test/support"]

  defp elixirc_paths(_env) do
    # gen/ holds the gitignored, generated proto modules (mirrors sdk/go/gen/).
    # It is produced by tools/generate-elixir-sdk.sh before build/test/lint,
    # exactly as sdk/go/gen/ is produced by tools/generate-go-sdk.sh.
    ["lib", "gen"]
  end

  defp deps do
    [
      # Generated proto types. Pinned ~> 0.17.0 to pick up the fix for
      # GHSA-rv48-qqj5-crxg (CVE-2026-54451); see tools/build-protoc-gen-elixir.sh.
      {:protobuf, "~> 0.17.0"},
      # HTTP client for the Connect application/proto transport.
      {:finch, "~> 0.19"},
      # Connect error envelope (always JSON, even over the binary proto transport).
      {:jason, "~> 1.4"},
      # OTel Erlang SDK: the Probe's tracer wraps the host TracerProvider and a
      # custom span processor captures ended spans for span_to_event (ADR-0002).
      {:opentelemetry_api, "~> 1.5"},
      {:opentelemetry, "~> 1.7"},
      # UUID v4 for request ids and idempotency keys (deliberate convenience).
      {:uuid, "~> 1.1"},
      # Property tests.
      {:stream_data, "~> 1.1", only: [:dev, :test]}
    ]
  end

  defp package do
    [
      licenses: ["MPL-2.0"],
      links: %{"GitHub" => @source_url}
    ]
  end

  defp description do
    "BEAM Probe SDK for Sentinel: filter projection, enforcement gate, " <>
      "Connect decision transport, and OTel-span-to-ProducerEvent emission."
  end
end
