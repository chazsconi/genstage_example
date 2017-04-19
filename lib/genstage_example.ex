defmodule GenStageExample do
  require Logger

  defmodule JobProducer do
    use GenStage
    defmodule State, do: defstruct counter: nil

    def start_link() do
      GenStage.start_link(__MODULE__,0, name: __MODULE__)
    end

    def init(counter) do
      Logger.info "JobProducer: #{inspect self()} starting"
      {:producer, %State{counter: counter}}
    end

    def handle_call({:notify, events}, _from, state) do
      {:reply, :ok, events, state} # Dispatch immediately
    end

    def handle_demand(demand, %State{counter: counter}=state) when demand > 0 do
      Logger.info "JobProducer: handle_demand demand: #{demand}"
      {:noreply, [], %State{counter: counter + demand}}
    end
  end

  defmodule ScraperWorkerConsumer do
    use GenStage

    def start_link(id) do
      name = :"#{__MODULE__}#{id}"
      Logger.info "start_link name: #{name}"
      GenStage.start_link(__MODULE__, [], name: name)
    end

    def init(_args) do
      Logger.info "ScraperWorkerConsumer init() pid: #{inspect self()}"
      {:producer_consumer,
        :does_not_have_state,
        # subscribe_to:
        #   [{GenStageExample.JobProducer, max_demand: 1}]
      }
    end

    def handle_call(:my_call, _from, state) do
      {:reply, :ok, [], state}
    end

    def handle_subscribe(type, _opts, from, state) do
      Logger.info "handle_subscribe type: #{type} #{inspect self()} from: #{inspect from}"
      {:automatic, state}
    end

    def handle_events([event], _from, state) do
      Logger.info "ScraperWorker handle_events #{inspect self()} event: #{inspect event}"
      #:timer.sleep(500) # Simulate HTTP request
      output_events =
        case event do
          {:details, v} ->
            [{:update_db, v}]

          {:summaries, min, max} ->
            if min == max do
              GenStage.call(GenStageExample.JobProducer,{:notify, [{:details, min}]})
            else
              mid = div(max - min, 2) + min
              split = [{:summaries, min, mid}, {:summaries, mid + 1, max}]
              Logger.info "Split #{min}, #{max} to #{inspect split}"
              GenStage.call(GenStageExample.JobProducer,{:notify, split})
            end
            []
        end
      {:noreply, output_events, state}
    end
  end

  defmodule DBUpdaterConsumer do
    use GenStage

    def start_link(scraper_worker_count) do
      GenStage.start_link(__MODULE__, scraper_worker_count, name: __MODULE__)
    end

    def init(scraper_worker_count) do
      scraper_workers =
        for id <- 1..scraper_worker_count do
          {:"Elixir.GenStageExample.ScraperWorkerConsumer#{id}", max_demand: 5}
        end
        Logger.info "DBUpdater init() pid: #{inspect self()}, scraper_workers: #{inspect scraper_workers}"

      {:consumer,
        :does_not_have_state,
        subscribe_to: scraper_workers
      }
    end

    def handle_events(events, _from, state) do
      Logger.info "DBUpdaterConsumer handle_events #{inspect self()} event: #{inspect events}"
      {:noreply, [], state}
    end
  end

  defmodule TestGenServer do
    use GenServer

    def start_link(scraper_worker_count) do
      GenServer.start_link(__MODULE__, scraper_worker_count, name: __MODULE__)
    end

    def handle_call(:my_call, _from, state) do
      {:reply, :ok, state}
    end
  end

  use Application

  @scraper_count 1
  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    if System.get_env("OBSERVER") do
      _ = Logger.info "Starting observer"
      :observer.start()
    end

    _ = Logger.info "Starting application"

    children =
      # [
      #   worker(GenStageExample.JobProducer, [])
      # ] ++
      for i <- 1..@scraper_count do
        worker(GenStageExample.TestGenServer, [i])
      end
      # ++ [
      #   worker(GenStageExample.DBUpdaterConsumer, [@scraper_count])
      # ]

    opts = [strategy: :one_for_one, name: GenStageExample.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
