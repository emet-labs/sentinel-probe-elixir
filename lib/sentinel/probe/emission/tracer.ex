defmodule Sentinel.Probe.SDK.Emission.Tracer do
  @moduledoc """
  Bundles the tracer a Probe emits with and the conversion into `ProducerEvent`s.
  Elixir analog of `sdk/go/emission/tracer.go` (`otel-bridge.ts`).

  A `ProbeTracer` wraps an EXISTING TracerProvider and NEVER starts its own:
  ADR-0002 makes OTel an adapter rather than the substrate, so the host owns the
  provider (and the exporter). The host builds a TracerProvider with the
  `Sentinel.Probe.SDK.Emission.Processor` span processor attached, then hands it
  here. No exporter is configured by the SDK.

  ## Divergence from the Go analog

  The Go `NewProbeTracer` builds a `TracerProvider` (resource, options) itself.
  On BEAM the OTel Erlang SDK's TracerProvider is a global, registered process
  (`otel_tracer_provider`); the SDK does not own its lifecycle. So `new/1`
  accepts the provider (config map or PID/name) and merely gets a tracer from
  it, leaving construction to the host. This mirrors the plan's directive that
  the tracer accepts an existing provider and never starts its own.
  """

  alias Sentinel.Model.V1.ProducerEvent
  alias Sentinel.Probe.SDK.Emission.{Span, SpanToEvent}

  defstruct [:tracer, :provider]

  @type provider_ref :: map() | pid() | atom()
  @type options :: %__MODULE__.Options{
          tracer_name: String.t(),
          provider: provider_ref()
        }

  defmodule Options do
    @moduledoc false
    defstruct [:tracer_name, :provider]
  end

  @type t :: %__MODULE__{
          tracer: term(),
          provider: provider_ref()
        }

  @doc """
  Builds a `ProbeTracer` over an existing TracerProvider.

  `provider` is the OTel provider the host built (a config map or a registered
  name/PID). `tracer_name` identifies the instrumentation scope. The host must
  have registered the `Emission.Processor` span processor with that provider for
  `on_end/2` to fire; this function does not start a provider or attach a
  processor.
  """
  @spec new(options()) :: t()
  def new(%Options{tracer_name: tracer_name, provider: provider}) do
    name = if is_atom(tracer_name), do: tracer_name, else: String.to_atom(tracer_name)
    tracer = :otel_tracer_provider.get_tracer(provider, name, :undefined, :undefined)
    %__MODULE__{tracer: tracer, provider: provider}
  end

  @doc "The underlying OTel tracer a Probe emits with."
  @spec tracer(t()) :: term()
  def tracer(%__MODULE__{tracer: tracer}), do: tracer

  @doc "The TracerProvider this tracer was built over (host-owned)."
  @spec provider(t()) :: provider_ref()
  def provider(%__MODULE__{provider: provider}), do: provider

  @doc """
  Converts an ended span (`Span`) into a `ProducerEvent`.

  Convenience wrapper over `SpanToEvent.span_to_event/1`, mirroring the
  reference's `ProbeTracer.toEvent`. The host builds the `%Span{}` — typically by
  capturing ended-span data through the `Emission.Processor` `on_end/2` callback
  — then calls this.
  """
  @spec to_event(Span.t()) :: ProducerEvent.t()
  def to_event(%Span{} = span), do: SpanToEvent.span_to_event(span)
end
