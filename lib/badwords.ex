defmodule Badwords do

  def file_list do
    dir = Application.app_dir(:badwords, "priv/books/")
    for f <- File.ls!(dir), do: Path.join(dir, f)
  end

  def word_list do
    {:ok, f} = File.read Application.app_dir(:badwords, "priv/badwords.txt")
    f |> String.split("\n", trim: true)
  end

  def validate do
    r_gt = greedy_task()
    IO.puts "Greedy Task #{length(Map.keys(r_gt))} keys, #{Enum.sum(Map.values(r_gt))} sum"
    r_gff = greedy_flow_by_file()
    IO.puts "Greedy Flow By File #{length(Map.keys(r_gff))} keys, #{Enum.sum(Map.values(r_gff))} sum"
    r_g = greedy_serial()
    IO.puts "Greedy #{length(Map.keys(r_g))} keys, #{Enum.sum(Map.values(r_g))} sum"
    r_s = stream_serial()
    IO.puts "Stream #{length(Map.keys(r_s))} keys, #{Enum.sum(Map.values(r_s))} sum"
  end

  def run do
    validate()
    Benchee.run(%{
          "greedy_task" => fn() -> greedy_task() end,
          "greedy_by_file" => fn() -> greedy_flow_by_file() end,
          "greedy_serial" => fn() -> greedy_serial() end,
          "stream_serial" => fn() -> stream_serial() end,
    })
  end


  def greedy_serial do
    wl = word_list()
    Enum.flat_map(file_list(), fn(filename) ->
      filename
      |> File.read!
      |> String.split("\n")
      |> Enum.map(& process_line(&1, wl))
    end)
    |> Enum.reduce(%{}, fn(m, map) -> Map.merge(map, m, fn(_k, v1, v2) -> v1 + v2 end) end)
  end

  def stream_serial do
    wl = word_list()
    Stream.flat_map(file_list(), fn(filename) ->
      filename
      |> File.stream!
      |> Stream.map(& process_line(&1, wl))
    end)
    |> Enum.reduce(%{}, fn(m, map) -> Map.merge(map, m, fn(_k, v1, v2) -> v1 + v2 end) end)
  end

  def greedy_task do
    wl = word_list()
    tasks = Task.async_stream(file_list(), fn filename ->
      IO.puts "TASK #{inspect self()}, calculating file #{filename}"
          filename
          |> File.read!
          |> String.split("\n")
          |> Enum.map(& process_line(&1, wl))
          |> Enum.reduce(%{}, fn(m, map) -> Map.merge(map, m, fn(_k, v1, v2) -> v1 + v2 end) end)
    end, timeout: 15_000)

    tasks
    |> Enum.reduce(%{}, fn({:ok, m}, map) -> Map.merge(map, m, fn(_k, v1, v2) -> v1 + v2 end) end)
  end

  def greedy_flow_by_file do
    wl = word_list()
    Flow.flat_map(Flow.from_enumerable(file_list(), max_demand: 1), fn(filename) ->
      IO.puts "FLOW #{inspect self()}, calculating file #{filename}"
      filename
      |> File.read!
      |> String.split("\n")
      |> Enum.map(& process_line(&1, wl))
      |> Enum.reduce(%{}, fn(m, map) -> Map.merge(map, m, fn(_k, v1, v2) -> v1 + v2 end) end)
    end)
    |> Flow.partition()
    #|> Enum.reduce(%{}, fn(m, map) -> Map.merge(map, m, fn(_k, v1, v2) -> v1 + v2 end) end)
    |> Enum.reduce(%{}, fn({word, c}, acc) -> Map.update(acc, word, 1, & &1 + c) end)
    #|> Flow.reduce(fn -> %{} end, fn({word, c}, acc) -> Map.update(acc, word, 1, & &1 + c) end)
    |> IO.inspect
    |> Map.new()
    |> IO.inspect
  end

  def process_line(s, wl) do
    s
    |> String.downcase
    |> String.split(" ")
    |> Enum.filter(&String.contains?(&1, wl))
    |> Enum.reduce(%{}, fn(word, acc) -> Map.update(acc, word, 1, & &1 + 1) end)
  end

end


