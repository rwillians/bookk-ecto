defmodule DocsTest do
  use ExUnit.Case, async: true

  doctest Bookk.Ecto.UUIDv7
  doctest Bookk.Options
end
