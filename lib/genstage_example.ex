# Usage: mix run lib/genstage_example.exs
#
# Hit Ctrl+C twice to stop it.
#
# This is a base example where a producer A emits items,
# which are amplified by a producer consumer B and printed
# by consumer C.
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
      {:producer, %State{counter: counter}, dispatcher: GenStage.BroadcastDispatcher}
    end

    def handle_demand(demand, %State{counter: counter}=state) when demand > 0 do
      Logger.info "JobProducer: handle_demand demand: #{demand}"
      # If the counter is 3 and we ask for 2 items, we will
      # emit the items 3 and 4, and set the state to 5.
      events =
        Enum.to_list(counter..counter+demand-1)
        |> Enum.map(fn n ->
          case rem(n,2) do
            0 -> {:summary, n}
            1 -> {:details, n}
          end
        end)
      {:noreply, events, %State{counter: counter + demand}}
    end
  end

  defmodule SummariesWorkerSupervisor do
    use ConsumerSupervisor

    def start_link() do
      children = [
        worker(GenStageExample.SummariesWorker, [], restart: :temporary)
      ]

      {:ok, pid} = ConsumerSupervisor.start_link(children, strategy: :one_for_one,
                                              subscribe_to: [{JobProducer,
                                              max_demand: 5,
                                              selector: fn {type, _data} -> type == :summary end}],
                                              name: __MODULE__)
      Logger.info "SummariesWorkerSupervisor start_link() created_pid: #{inspect pid}"
      {:ok, pid}
    end
  end

  defmodule SummariesWorker do
    def start_link(event) do
      Task.start_link(fn ->
        Logger.info "SummariesWorker Task.start_link #{inspect self()} event: #{inspect event}"
        :timer.sleep(2000)
      end)
    end
  end

  defmodule DetailsWorkerSupervisor do
    use ConsumerSupervisor

    def start_link() do
      children = [
        worker(GenStageExample.DetailsWorker, [], restart: :temporary)
      ]

      {:ok, pid} = ConsumerSupervisor.start_link(children, strategy: :one_for_one,
                                              subscribe_to: [{JobProducer,
                                              max_demand: 5,
                                              selector: fn {type, _data} -> type == :details end}],
                                              name: __MODULE__)
      Logger.info "DetailsWorkerSupervisor start_link() created_pid: #{inspect pid}"
      {:ok, pid}
    end
  end

  defmodule DetailsWorker do
    def start_link(event) do
      Task.start_link(fn ->
        Logger.info "DetailsWorker Task.start_link #{inspect self()} event: #{inspect event}"
        :timer.sleep(2000)
      end)
    end
  end


  # defmodule B do
  #   use GenStage
  #   defmodule State, do: defstruct number: nil
  #
  #   def init(number) do
  #     {:producer_consumer, %State{number: number}}
  #   end
  #
  #   def handle_events(events, _from, %State{number: number}=state) do
  #     Logger.info "B: handle_events count: #{length(events)}"
  #     # If we receive [0, 1, 2], this will transform
  #     # it into [0, 1, 2, 1, 2, 3, 2, 3, 4].
  #     events =
  #       for event <- events,
  #           entry <- event..event+number,
  #           do: entry
  #     {:noreply, events, state}
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

  def start_scraper do
    {:ok, job_producer} = GenStage.start_link(GenStageExample.JobProducer,0, name: GenStageExample.JobProducer)
    {:ok, summaries_worker_supervisor} = GenStageExample.SummariesWorkerSupervisor.start_link()
  end

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
      supervisor(GenStageExample.SummariesWorkerSupervisor,[]),
      supervisor(GenStageExample.DetailsWorkerSupervisor,[])
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
