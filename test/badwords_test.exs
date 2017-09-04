defmodule BadwordsTest do
  use ExUnit.Case
  doctest Badwords

  test "greets the world" do
    assert Badwords.hello() == :world
  end
end
