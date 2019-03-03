defmodule Chord.Node do

  defmodule Peer do
    defstruct name: nil, hash: nil
  end

  use GenServer

  # Client

  def start_link(arg) do
    name = {:via, Registry, {ChordSimulator.Registry, "node_#{Enum.at(arg, 1)}"}}
    GenServer.start_link(__MODULE__, arg |> Enum.at(0) |> Map.put(:id, Enum.at(arg, 1)), name: name)
  end

  # Server (callbacks)

  def init(arg) do
    GenServer.cast(self(), :start)
    myHash = get_hash("node_#{arg.id}")
    fail_node = false
    status = not fail_node
    finger = Enum.reduce(1..40, [], fn(_, acc) -> [nil|acc] end)
    params = Map.merge(arg, %{self: %Peer{name: "node_#{arg.id}", hash: myHash},predecessor: nil,successor: nil,finger: finger,fail_node: fail_node,total_hops: 0,message_sent: 0,alive: status})
    {:ok, params}
  end

  def handle_cast(:start, params) do
    entry_node = GenServer.call(Chord.Manager, :initial_insert)
    new_params = if entry_node == nil, do: create(params), else: join(params, entry_node)
    :ok = GenServer.call(Chord.Manager, {:create_ring, new_params.self})
    {:noreply, new_params}
  end

  def handle_cast(:all_node_joined, params) do
    Process.send_after(self(), :stabilize, 50)
    Process.send_after(self(), :check_predecessor, 50)
    Process.send_after(self(), {:fix_fingers, 0}, 20)
    Process.send_after(self(), :message, 5000)
    {:noreply, params}
  end

  def handle_cast({:find_successor, id, callee, hops}, params) do
    find_successor(params, id, callee, hops + 1)
    {:noreply, params}
  end

  def handle_cast(:terminate, params) do
    new_params = Map.put(params, :alive, false)
    {:stop, :normal, new_params}
  end

  def handle_call({:new_params, key, value}, _from, params), do: {:reply, :ok, Map.put(params, key, value)}

  def handle_call({:find_successor, id}, from, params) do
    find_successor(params, id, from, 1)
    {:noreply, params}
  end

  def handle_call(:predecessor, _from, params), do: {:reply, params.predecessor, params}

  def handle_call({:notify, node}, _from, params), do: {:reply, :ok, notify(params, node)}

  def handle_info(:stabilize, params) do
    if params.alive and not params.fail_node, do: spawn_link(fn() -> stabilize(params) end)
    {:noreply, params}
  end

  def handle_info(:check_predecessor, params) do
    if params.alive and not params.fail_node, do: spawn_link(fn() -> check_predecessor(params) end)
    {:noreply, params}
  end

  def handle_info({:fix_fingers, next}, params) do
    if params.alive and not params.fail_node, do: spawn_link(fn() -> fix_fingers(params, next) end)
    {:noreply, params}
  end

  def handle_info(:message, params) do
    cond do
      params.message_sent == params.num_requests ->
        average_hop = params.total_hops / params.num_requests
        :ok = GenServer.call(Chord.Manager, {:node_completed, params.self.name, average_hop})
      not params.fail_node ->
        message = Enum.reduce(1..10, "", fn(_, acc) -> acc <> <<Enum.random(0..255)>> end)
        message_hash = get_hash(message)
        spawn_link(fn() -> send_message(params, message_hash) end)
        Process.send_after(self(), :message, 1000)
    end
    {:noreply, params}
  end

  def terminate(reason, _params) do
    if reason != :normal, do: IO.inspect(reason)
    :timer.sleep(1000)
  end

  # Aux

  defp find_successor(params, id) do
    if ring_traversal?(id, params.self.hash, false, params.successor.hash, true) do
      {params.successor, 0}
    else
      cl_node = closest_preceding_node(params, id, 40 - 1)
      if cl_node == params.self do
        {params.self, 0} # TODO
      else
        GenServer.call({:via, Registry, {ChordSimulator.Registry, cl_node.name}}, {:find_successor, id})
      end
    end
  end

  defp find_successor(params, id, callee, hops) do
    if ring_traversal?(id, params.self.hash, false, params.successor.hash, true) do
      GenServer.reply(callee, {params.successor, hops})
    else
      cl_node = closest_preceding_node(params, id, 40 - 1)
      if cl_node == params.self do
        GenServer.reply(callee, {params.self, hops}) # TODO
      else
        GenServer.cast({:via, Registry, {ChordSimulator.Registry, cl_node.name}}, {:find_successor, id, callee, hops})
      end
    end
  end

  defp closest_preceding_node(params, _id, i) when i < 0, do: params.self

  defp closest_preceding_node(params, id, i) do
    if Enum.at(params.finger, i) == nil do
      params.self
    else
      if ring_traversal?(Enum.at(params.finger, i).hash, params.self.hash, false, id, false), do: Enum.at(params.finger, i), else: closest_preceding_node(params, id, i - 1)
    end
  end

  defp create(params) do
    Map.merge(params, %{predecessor: nil, successor: params.self})
  end

  defp join(params, entry_node) do
    {successor, _} = GenServer.call({:via, Registry, {ChordSimulator.Registry, entry_node.name}}, {:find_successor, params.self.hash})
    Map.merge(params, %{predecessor: nil, successor: successor})
  end

  defp stabilize(params) do
    x = GenServer.call({:via, Registry, {ChordSimulator.Registry, params.successor.name}}, :predecessor)

    updated_successor =
      if x != nil and ring_traversal?(x.hash, params.self.hash, false, params.successor.hash, false) do
        :ok = GenServer.call({:via, Registry, {ChordSimulator.Registry, params.self.name}}, {:new_params, :successor, x})
        x
      else
        params.successor
      end

    :ok = GenServer.call({:via, Registry, {ChordSimulator.Registry, updated_successor.name}}, {:notify, params.self})

    Process.send_after(Registry.lookup(ChordSimulator.Registry, params.self.name) |> Enum.at(0) |> elem(0), :stabilize, 50)
  end

  defp notify(params, node) do
    if params.predecessor == nil or ring_traversal?(node.hash, params.predecessor.hash, false, params.self.hash, false) do
      Map.put(params, :predecessor, node)
    else
      params
    end
  end

  defp fix_fingers(params, next) do
    <<id_int::40>> = params.self.hash
    id_int = rem(id_int + trunc(:math.pow(2, next)), trunc(:math.pow(2, 40)))
    id = <<id_int::40>>
    {successor, _} = find_successor(params, id)
    updated_finger = List.replace_at(params.finger, next, successor)
    :ok = GenServer.call({:via, Registry, {ChordSimulator.Registry, params.self.name}}, {:new_params, :finger, updated_finger})

    next = if next + 1 >= 40, do: 0, else: next + 1
    Process.send_after(Registry.lookup(ChordSimulator.Registry, params.self.name) |> Enum.at(0) |> elem(0), {:fix_fingers, next}, 20)
  end

  defp check_predecessor(params) do
    if params.predecessor != nil and GenServer.whereis({:via, Registry, {ChordSimulator.Registry, params.predecessor.name}}) == nil do
      :ok = GenServer.call({:via, Registry, {ChordSimulator.Registry, params.self.name}}, {:new_params, :predecessor, nil})
    end
    Process.send_after(Registry.lookup(ChordSimulator.Registry, params.self.name) |> Enum.at(0) |> elem(0), :check_predecessor, 50)
  end

  defp send_message(params, message_hash) do
    {_result, hops} = find_successor(params, message_hash)
    :ok = GenServer.call({:via, Registry, {ChordSimulator.Registry, params.self.name}}, {:new_params, :total_hops, params.total_hops + hops})
    :ok = GenServer.call({:via, Registry, {ChordSimulator.Registry, params.self.name}}, {:new_params, :message_sent, params.message_sent + 1})
  end

  defp ring_traversal?(target, predecess, pred_range, success, succ_range) do
    cond do
      predecess < success ->
        if target >= predecess and target <= success do
          if (target == predecess and not pred_range) or (target == success and not succ_range), do: false, else: true
        else
          false
        end
      predecess == success ->
        if target == predecess and (not pred_range or not succ_range), do: false, else: true
      predecess > success ->
        if target > success and target < predecess do
          false
        else
          if (target == predecess and not pred_range) or (target == success and not succ_range), do: false, else: true
        end
    end
  end

  defp get_hash(input) do
    remain_digest = 160 - 40
    <<_::size(remain_digest), x::40>> = :crypto.hash(:sha, input)
    <<x::40>>
  end
end
