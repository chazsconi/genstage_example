defmodule GenStageExample.TestGenServer do
  use GenServer

  def start_link() do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def handle_call(:my_call, _from, state) do
    {:reply, :ok, state}
  end
end
