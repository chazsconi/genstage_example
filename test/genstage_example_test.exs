defmodule GenstageExampleTest do
  use ExUnit.Case

  @tag timeout: 600_000
  test "bad_call2" do
    delay = 500 # 1800 crashes after 6, 1500 after 4

    for n <- 1..100 do
      IO.puts "Attempt #{n}"

      catch_exit(GenStage.call(GenStageExample.TestGenServer, :foo))
      :timer.sleep(delay)
      assert :ok == GenStage.call(GenStageExample.TestGenServer,:my_call)
    end
  end
end
