defmodule Sentinel.Probe.SDK.OtlpRoundTripTest do
  use ExUnit.Case, async: false

  alias Sentinel.Model.V1.SequenceCoordinate
  alias Sentinel.Probe.SDK.Emission.Span
  alias Sentinel.Probe.SDK.Otlp.{Codec, Http, Receiver}
  alias Sentinel.Probe.SDK.Emission.Tracer

  @start 1_700_000_000_000_000_000
  @schema "sagashop-contract-v1"
  @charge_id "01f0c0ffee-charge"
  @ship_id "01f0c0ffee-ship"
  @order_id "order-1001"
  @lone_ship_id "01f0c0ffee-noship"
  @declined_id "01f0c0ffee-declin"
  @lone_order_id "order-2002"
  @spec_id "sagashop.p4.ship-requires-prior-charge"

  test "a linked charge and ship return a verdict, and a ship without a charge is not Satisfy" do
    bin = periphery_bin()
    assert File.exists?(bin), "sentinel-periphery is missing at #{bin}"

    {receiver, port} = start_pair(bin)
    cover_and_bootstrap(port)

    assert {:ok, _} =
             Tracer.export_traces(
               [charge_span(@charge_id, @order_id, "success")],
               origin(port),
               "sagashop-payment"
             )

    assert Receiver.verdicts(receiver) == []

    assert {:ok, _} =
             Tracer.export_traces(
               [ship_span(@ship_id, @order_id, @charge_id)],
               origin(port),
               "sagashop-order"
             )

    verdicts = Receiver.verdicts(receiver)
    assert verdicts != []
    assert Enum.any?(verdicts, &String.contains?(&1.id, @spec_id))
    assert Enum.any?(verdicts, &(&1.outcome == :VERDICT_OUTCOME_SATISFY))

    stop_port(port)
    Receiver.stop(receiver)

    {receiver, port} = start_pair(bin)
    cover_and_bootstrap(port)

    assert {:ok, _} =
             Tracer.export_traces(
               [charge_span(@declined_id, @lone_order_id, "declined")],
               origin(port),
               "sagashop-payment"
             )

    assert {:ok, _} =
             Tracer.export_traces(
               [ship_span(@lone_ship_id, @lone_order_id, nil)],
               origin(port),
               "sagashop-order"
             )

    refused = Receiver.verdicts(receiver)
    assert refused != []
    assert Enum.all?(refused, &(&1.outcome != :VERDICT_OUTCOME_SATISFY))

    stop_port(port)
    Receiver.stop(receiver)
  end

  defp start_pair(bin) do
    {:ok, receiver} = Receiver.start_link()
    port_number = free_port()

    port =
      Port.open({:spawn_executable, bin}, [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        {:args, []},
        {:env,
         env(%{
           "SENTINEL_LISTEN" => "127.0.0.1:#{port_number}",
           "SENTINEL_STORE" => "memory",
           "SENTINEL_PROBE_LOG_URL" => Receiver.origin(receiver) <> "/v1/logs"
         })}
      ])

    await_listening(port, "", 150)
    {receiver, %{port: port, number: port_number}}
  end

  defp cover_and_bootstrap(%{number: number}) do
    for service <- ["sagashop-order", "sagashop-payment"] do
      signal = Codec.completeness_signal(7, 1, 1, @start)
      assert {:ok, _} = Tracer.export_completeness(signal, origin(number), service)
    end

    body =
      Jason.encode!(%{
        observed_at: %{sentinel_clock_id: "sentinel", nanoseconds: @start}
      })

    assert {:ok, _} =
             Http.post(origin(number), "/v1/filters/bootstrap-cat1", [], body,
               content_type: "application/json"
             )
  end

  defp charge_span(event_id, order_id, status) do
    sample_span(event_id, "sagashop.payment.charge.completed", nil, [
      {"sagashop.payment.status", {:string, status}},
      {"sagashop.order.id", {:string, order_id}}
    ])
  end

  defp ship_span(event_id, order_id, parent) do
    sample_span(event_id, "sagashop.order.ship.requested", parent, [
      {"sagashop.order.id", {:string, order_id}}
    ])
  end

  defp sample_span(event_id, name, parent, attributes) do
    %Span{
      event_id: event_id,
      name: name,
      sequence: %SequenceCoordinate{epoch: 7, sequence: 1},
      schema_version: @schema,
      acknowledged_epoch: 1,
      claimed_sensitivity: :SENSITIVITY_INTERNAL,
      claimed_capabilities: [],
      start_time: @start,
      attributes: attributes,
      parent_event_id: parent
    }
  end

  defp origin(number) when is_integer(number), do: "http://127.0.0.1:#{number}"
  defp origin(%{number: number}), do: origin(number)

  defp periphery_bin do
    System.get_env("SENTINEL_PERIPHERY_BIN") ||
      Path.expand("../../../target/debug/sentinel-periphery", __DIR__)
  end

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end

  defp env(overrides) do
    System.get_env()
    |> Map.merge(overrides)
    |> Enum.map(fn {key, value} -> {String.to_charlist(key), String.to_charlist(value)} end)
  end

  defp await_listening(_port, buffer, attempts) when attempts <= 0 do
    flunk("sentinel-periphery did not listen:\n#{buffer}")
  end

  defp await_listening(port, buffer, attempts) do
    receive do
      {^port, {:data, data}} ->
        buffer = buffer <> data

        if String.contains?(buffer, "listening on") do
          buffer
        else
          await_listening(port, buffer, attempts)
        end

      {^port, {:exit_status, status}} ->
        flunk("sentinel-periphery exited #{status}:\n#{buffer}")
    after
      100 ->
        await_listening(port, buffer, attempts - 1)
    end
  end

  defp stop_port(%{port: port}) do
    case Port.info(port, :os_pid) do
      {:os_pid, pid} -> System.cmd("kill", ["-TERM", Integer.to_string(pid)])
      _ -> :ok
    end

    try do
      Port.close(port)
    catch
      _, _ -> :ok
    end
  end
end
