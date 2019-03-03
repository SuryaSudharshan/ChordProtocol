defmodule ChordSimulatorTest do
  use ExUnit.Case
  doctest ChordProtocol

  test "greets the world" do
    assert ChordProtocol.hello() == :world
  end
end
