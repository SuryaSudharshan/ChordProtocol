defmodule Simulator do
  def start do
    {:ok, _} = Chord.Supervisor.start_link(self())
    receive do
      :done -> IO.write("")
    end
  end
end
