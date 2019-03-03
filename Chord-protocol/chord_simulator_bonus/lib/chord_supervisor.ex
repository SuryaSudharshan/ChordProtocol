defmodule Chord.Supervisor do
  use Supervisor

  def start_link(arg) do
    Supervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end

  def init(arg) do
    if Enum.at(System.argv(), 1) == nil do
      IO.puts "Insufficient Inputs"
      {:invalid_argv}
    else
      registry = {Registry, keys: :unique, name: Chord.Registry, partitions: System.schedulers_online()}

      total_nodes = System.argv() |> Enum.at(0) |> String.to_integer()
      num_requests = System.argv() |> Enum.at(1) |> String.to_integer()

      share_arg =
        if Enum.at(System.argv(), 2) do
          %{num_requests: num_requests, fail_mode: true, fail_rate: System.argv() |> Enum.at(2) |> String.to_integer()}
        else
          IO.puts("Invalid Arguments")
          System.halt(0)
        end

      manager = Supervisor.child_spec({Chord.Manager, Map.merge(share_arg, %{total_nodes: total_nodes, daemon: arg})}, restart: :transient)

      nodes = Enum.reduce(total_nodes..1, [],
        fn(x, acc) -> [Supervisor.child_spec({Chord.Node, [share_arg, x]}, id: {Chord.Node, x}, restart: :temporary) | acc] end)

      children = [registry | [manager | nodes]]

      Supervisor.init(children, strategy: :one_for_one)
    end
  end
end
