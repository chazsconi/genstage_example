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
      # If the counter is 3 and we ask for 2 items, we will
      # emit the items 3 and 4, and set the state to 5.
      events =
        Enum.to_list(counter..counter+demand-1)
        |> Enum.map(fn n ->
          case rem(n,3) do
            0 -> {:summary, n}
            _ -> {:details, n}
          end
        end)
      {:noreply, [], %State{counter: counter + demand}}
    end
  end

  # defmodule SummariesWorkerSupervisor do
  #   use ConsumerSupervisor
  #
  #   def start_link() do
  #     children = [
  #       worker(GenStageExample.SummariesWorker, [], restart: :temporary)
  #     ]
  #
  #     {:ok, pid} = ConsumerSupervisor.start_link(children, strategy: :one_for_one,
  #                                             subscribe_to: [{JobProducer,
  #                                             max_demand: 5,
  #                                             selector: fn {type, _data} -> type == :summary end}],
  #                                             name: __MODULE__)
  #     Logger.info "SummariesWorkerSupervisor start_link() created_pid: #{inspect pid}"
  #     {:ok, pid}
  #   end
  # end
  #
  # defmodule SummariesWorker do
  #   def start_link(event) do
  #     Task.start_link(fn ->
  #       Logger.info "SummariesWorker Task.start_link #{inspect self()} event: #{inspect event}"
  #       :timer.sleep(2000)
  #     end)
  #   end
  # end
  #
  # defmodule DetailsWorkerSupervisor do
  #   use ConsumerSupervisor
  #
  #   def start_link() do
  #     children = [
  #       worker(GenStageExample.DetailsWorker, [], restart: :temporary)
  #     ]
  #
  #     {:ok, pid} = ConsumerSupervisor.start_link(children, strategy: :one_for_one,
  #                                             subscribe_to: [{JobProducer,
  #                                             max_demand: 5,
  #                                             selector: fn {type, _data} -> type == :details end}],
  #                                             name: __MODULE__)
  #     Logger.info "DetailsWorkerSupervisor start_link() created_pid: #{inspect pid}"
  #     {:ok, pid}
  #   end
  # end
  #
  # defmodule DetailsWorker do
  #   def start_link(event) do
  #     Task.start_link(fn ->
  #       Logger.info "DetailsWorker Task.start_link #{inspect self()} event: #{inspect event}"
  #       :timer.sleep(2000)
  #     end)
  #   end
  # end


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
        subscribe_to:
          [{GenStageExample.JobProducer, max_demand: 1}]
      }
    end

    def handle_events([event], _from, state) do
      Logger.info "ScraperWorker handle_events #{inspect self()} event: #{inspect event}"
      :timer.sleep(500) # Simulate HTTP request
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

  # defmodule SummariesWorkerConsumer do
  #   use GenStage
  #
  #   def start_link(), do: GenStage.start_link(__MODULE__, [])
  #
  #   def init(_args) do
  #     Logger.info "SummariesWorkerConsumer init() pid: #{inspect self()}"
  #     {:consumer,
  #       :does_not_have_state,
  #       subscribe_to:
  #         [{GenStageExample.JobProducer, max_demand: 1, selector: fn {type, _data} -> type == :summary end}]
  #     }
  #   end
  #
  #   def handle_events(events, _from, state) do
  #     Logger.info "SummariesWorker handle_events #{inspect self()} event: #{inspect events}"
  #     :timer.sleep(2000)
  #     output_events = []
  #     {:noreply, output_events, state}
  #   end
  # end
  #
  # defmodule DetailsWorkerConsumer do
  #   use GenStage
  #
  #   def start_link(), do: GenStage.start_link(__MODULE__, [])
  #
  #   def init(_args) do
  #     Logger.info "DetailsWorkerConsumer init() pid: #{inspect self()}"
  #     {:consumer,
  #       :does_not_have_state,
  #       subscribe_to:
  #         [{GenStageExample.JobProducer, max_demand: 1, selector: fn {type, _data} -> type == :details end}]
  #     }
  #   end
  #
  #   def handle_events(events, _from, state) do
  #     Logger.info "DetailsWorker handle_events #{inspect self()} event: #{inspect events}"
  #     :timer.sleep(1000)
  #     output_events = []
  #     {:noreply, output_events, state}
  #   end
  # end

  #
  # defmodule C do
  #   use GenStage
  #
  #   def init(:ok) do
  #     {:consumer, :the_state_does_not_matter}
  #   end
  #
  #   def handle_events(events, _from, state) do
  #     Logger.info "C: handle_events count: #{length(events)}"
  #     # Wait for a second.
  #     :timer.sleep(1000)
  #
  #     # Inspect the events.
  #     Logger.info "C: events output: #{inspect events}"
  #
  #     # We are a consumer, so we would never emit items.
  #     {:noreply, [], state}
  #   end
  # end

  # def start_scraper do
  #   {:ok, job_producer} = GenStage.start_link(GenStageExample.JobProducer,0, name: GenStageExample.JobProducer)
  #   {:ok, summaries_worker_supervisor} = GenStageExample.SummariesWorkerSupervisor.start_link()
  # end

  use Application

  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    if System.get_env("OBSERVER") do
      _ = Logger.info "Starting observer"
      :observer.start()
    end

    _ = Logger.info "Starting application"

    children = [
      worker(GenStageExample.JobProducer, []),
      # worker(GenStageExample.SummariesWorkerConsumer, [], id: :s1),
      # worker(GenStageExample.SummariesWorkerConsumer, [], id: :s2),
      # worker(GenStageExample.DetailsWorkerConsumer, [], id: :d1),
      # worker(GenStageExample.DetailsWorkerConsumer, [], id: :d2)
      worker(GenStageExample.ScraperWorkerConsumer, [1], id: :s1),
      worker(GenStageExample.ScraperWorkerConsumer, [2], id: :s2),
      worker(GenStageExample.ScraperWorkerConsumer, [3], id: :s3),
      worker(GenStageExample.ScraperWorkerConsumer, [4], id: :s4),
      #worker(GenStageExample.ScraperWorkerConsumer, [], id: :s2),
      worker(GenStageExample.DBUpdaterConsumer, [4], id: :db1)
      # supervisor(GenStageExample.SummariesWorkerSupervisor,[]),
      # supervisor(GenStageExample.DetailsWorkerSupervisor,[])
    ]
    opts = [strategy: :one_for_one, name: GenStageExample.Supervisor]
    Supervisor.start_link(children, opts)
  end


  # def start do
  # {:ok, a} = GenStage.start_link(A, 0)   # starting from zero
  # {:ok, b} = GenStage.start_link(B, 2)   # expand by 2
  # {:ok, c} = GenStage.start_link(C, :ok) # state does not matter
  #
  # GenStage.sync_subscribe(b, to: a, min_demand: 2, max_demand: 4)
  # GenStage.sync_subscribe(c, to: b)
  # # Process.sleep(:infinity)
  # end
end
