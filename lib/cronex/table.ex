defmodule Cronex.Table do
  @moduledoc """
  This module represents a cron table.
  """

  use GenServer
  import Cronex.Job
  alias Cronex.Job
  require Logger

  # Interface functions
  @doc """
  Starts a `Cronex.Table` instance.

  `args` must contain a `:scheduler` with a valid `Cronex.Scheduler`.
  """
  def start_link(args, opts \\ []) do
    GenServer.start_link(__MODULE__, args, opts)
  end

  @doc false
  def add_job(pid, %Job{} = job) do
    GenServer.call(pid, {:add_job, job})
  end

  @doc false
  def get_jobs(pid) do
    GenServer.call(pid, :get_jobs)
  end

  # Callback functions
  def init(args) do
    scheduler = args[:scheduler]

    if is_nil(scheduler), do: raise_scheduler_not_provided_error()

    GenServer.cast(self(), :init)

    state = %{
      scheduler: scheduler,
      jobs: Map.new(),
      timer: new_ping_timer(),
      leader: false
    }

    {:ok, state}
  end

  def handle_cast(:init, %{scheduler: scheduler} = state) do
    # Load jobs
    new_state =
      scheduler.jobs
      |> Enum.reduce(state, fn job, state ->
        job = apply(scheduler, job, [])
        do_add_job(state, job)
      end)

    # Try to become leader
    new_state = try_become_leader(new_state)

    {:noreply, new_state}
  end

  def handle_call({:add_job, %Job{} = job}, _from, state) do
    new_state = state |> do_add_job(job)
    {:reply, :ok, new_state}
  end

  def handle_call(:get_jobs, _from, state) do
    {:reply, state[:jobs], state}
  end

  def handle_call(:new_leader, _from, state) do
    {:reply, :ok, Map.put(state, :leader, false)}
  end

  def handle_info(:ping, %{leader: false} = state), do: {:noreply, state}

  def handle_info(:ping, %{scheduler: scheduler} = state) do
    updated_timer = new_ping_timer()

    updated_jobs =
      for {id, job} <- state[:jobs], into: %{} do
        updated_job =
          if job |> can_run? do
            job |> run(scheduler.job_supervisor)
          else
            job
          end

        {id, updated_job}
      end

    new_state = %{state | timer: updated_timer, jobs: updated_jobs}
    {:noreply, new_state}
  end

  def return_pid(), do: self()

  defp raise_scheduler_not_provided_error do
    raise ArgumentError,
      message: """
      No scheduler was provided when starting Cronex.Table.

      Please provide a Scheduler like so:

          Cronex.Table.start_link(scheduler: MyApp.Scheduler)
      """
  end

  # take first node from the list and tries to make it leader
  defp try_become_leader(%{scheduler: scheduler} = state) do
    [leader | tail] = node_list = Application.get_env(:cronex, :node_list, [Node.self()])
    leader_table_pid = :rpc.call(leader, __MODULE__, :return_pid, [])
    Logger.info("Cronex leader node: #{inspect(leader)}, pid: #{inspect(leader_table_pid)}")

    trans_result =
      :global.trans(
        {:leader, leader_table_pid},
        fn ->
          case GenServer.multi_call(tail, scheduler.table, :new_leader) do
            {_, []} -> :ok
            _ -> :aborted
          end
        end,
        node_list,
        0
      )

    case trans_result do
      :ok -> Map.put(state, :leader, true)
      :aborted -> Map.put(state, :leader, false)
    end
  end

  defp do_add_job(state, %Job{} = job) do
    index = state[:jobs] |> Map.keys() |> Enum.count()
    put_in(state, [:jobs, index], job)
  end

  defp new_ping_timer, do: Process.send_after(self(), :ping, ping_interval())

  defp ping_interval, do: Application.get_env(:cronex, :ping_interval, 30000)
end
