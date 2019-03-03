defmodule Chord.Supervisor do
  use Supervisor

  def start_link(arg) do
    Supervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end

  def init(arg) do
    if Enum.at(System.argv(), 1) == nil or Enum.at(System.argv(),2) do
      IO.puts "Invalid arguments. Please try again with 'mix run lib/proj3.exs numnodes numrequests'"
      {:invalid_argv}
    else
      registry = {Registry, keys: :unique, name: ChordSimulator.Registry, partitions: System.schedulers_online()}

      total_nodes = System.argv() |> Enum.at(0) |> String.to_integer()
      num_requests = System.argv() |> Enum.at(1) |> String.to_integer()

      failure =
          %{num_requests: num_requests, fail_mode: false}

      manager = Supervisor.child_spec({Chord.Manager, Map.merge(failure, %{total_nodes: total_nodes, daemon: arg})}, restart: :transient)

      nodes = Enum.reduce(total_nodes..1, [],
        fn(x, acc) -> [Supervisor.child_spec({Chord.Node, [failure, x]}, id: {Chord.Node, x}, restart: :temporary) | acc] end)

      children = [registry | [manager | nodes]]

      Supervisor.init(children, strategy: :one_for_one)
    end
  end
end
