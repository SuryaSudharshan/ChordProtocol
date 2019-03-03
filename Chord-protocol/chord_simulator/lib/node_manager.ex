defmodule Chord.Manager do
  use GenServer

  def start_link(arg) do
    GenServer.start_link(__MODULE__, arg, name: Chord.Manager)
  end

  def init(arg) do
    nodes_checklist = 
                      Enum.reduce(1..arg.total_nodes, %{}, fn(x, acc) -> Map.put(acc, "node_#{x}", false) end)
    params = Map.merge(arg, %{
      joined_nodes: %{},
      joined_nodes_count: 0,
      nodes_checklist: nodes_checklist,
      nodes_finished: 0,
      node_join_progess: 0
    })
    {:ok, params}
  end

  def handle_call(:initial_insert, _from, params) do
    if params.joined_nodes_count == 0 do
      {:reply, nil, params}
    else
      index = :rand.uniform(params.joined_nodes_count)
      random_node = params.joined_nodes[index]
      {:reply, random_node, params}
    end
  end

  def handle_call({:create_ring, node}, _from, params) do
    additional_nodes = Map.put(params.joined_nodes, params.joined_nodes_count + 1, node)
    params_new = Map.merge(params, %{joined_nodes: additional_nodes, joined_nodes_count: params.joined_nodes_count + 1})
    if params_new.joined_nodes_count == params_new.total_nodes do
      IO.puts("Spawning Actors..")
      :timer.sleep(1000)
      IO.puts "Chord Ring Successfully Created"
      Enum.each(Map.values(params_new.joined_nodes), fn(x) -> GenServer.cast({:via, Registry, {ChordSimulator.Registry, x.name}}, :all_node_joined) end)
      IO.puts "Starting lookup requests..."
    end
    {:reply, :ok, params_new}
  end

  def handle_call({:node_completed, node, average_hop}, from, params) do
    GenServer.reply(from, :ok)

    if params.nodes_checklist[node] == false do
      new_nodes_checklist = Map.put(params.nodes_checklist, node, average_hop)
      params_new = Map.merge(params, %{nodes_checklist: new_nodes_checklist, nodes_finished: params.nodes_finished + 1})
      if params_new.nodes_finished == params_new.total_nodes do
        Enum.each(1..params_new.total_nodes, fn(x) -> GenServer.cast({:via, Registry, {ChordSimulator.Registry, "node_#{x}"}}, :terminate) end)
        :timer.sleep(2000)
        {:stop, :normal, params_new}
      else
        {:noreply, params_new}
      end
    else
      {:noreply, params}
    end
  end

  def terminate(reason, params) do
    if reason == :normal do
      average_hop = Enum.reduce(Map.values(params.nodes_checklist), 0, fn(x, acc) -> x + acc end) / params.total_nodes
      IO.puts "Average hops: #{average_hop}"
    else
      IO.inspect(reason)
    end
    send(params.daemon, :done)
  end
end
