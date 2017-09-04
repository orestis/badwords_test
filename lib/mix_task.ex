defmodule Mix.Tasks.Badwords do
  use Mix.Task

  def run(_) do
    Badwords.run()
  end
end
