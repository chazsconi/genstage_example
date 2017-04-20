defmodule GenStageExample do
  require Logger
  use Application

  def start(_type, _args) do
    _ = Logger.info "Starting application"
    GenStageExample.TestSupervisor.start_link()
  end
end
