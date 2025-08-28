# Copyright 2024 Ryan Winchester
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation files
# (the “Software”), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
# BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# See {https://github.com/ryanwinchester/uuidv7}.
#
# " Consolidated Ryan's work into a single module under a non-obstrusive
#   module name. I didn't want to impose that whoever's using Bookk
#   should have to use Ryan's module - like, maybe they want to use a
#   different library that's also named UUIDv7 what would cause a
#   module name collision. This way, the lib is contained under Bookk's
#   namespace and the user is free to use other libraries named
#   UUIDv7. " — Rafael Willians
defmodule Bookk.Ecto.UUIDv7 do
  @moduledoc ~S"""
  UUIDv7 Ecto type implementaiton using submillisecond clock
  precision.

  ## Examples

      Bookk.Ecto.UUIDv7.generate()
      #> "018e90d8-06e8-7f9f-bfd7-6730ba98a51b"

      Bookk.Ecto.UUIDv7.bingenerate()
      #> <<1, 142, 144, 216, 6, 232, 127, 159, 191, 215, 103, 48, 186, 152, 165, 27>>

  """

  use Ecto.Type

  @typedoc ~S"""
  A hex-encoded UUID string.
  """
  @type t :: <<_::288>>

  @typedoc ~S"""
  A raw binary representation of a UUID.
  """
  @type raw :: <<_::128>>

  @version 7
  @variant 2

  # For macOS (Darwin) or Windows we would normally use 10 bits
  # instead of 12. However, it would be an extra complexity and
  # tradeoff of checking OS at runtime with some extra calcs, just for
  # 2 bits of extra randomness for people running their applications
  # on Windows or macOS.
  @sub_ms_bits 12

  @possible_values Bitwise.bsl(1, @sub_ms_bits)

  @ns_per_ms 1_000_000

  @minimal_step_ns div(@ns_per_ms, @possible_values) + 1

  @doc ~S"""
  Generates a version 7 UUID using submilliseconds for increased clock
  precision.

  ## Example

      Bookk.Ecto.UUIDv7.generate()
      #> "018e90d8-06e8-7f9f-bfd7-6730ba98a51b"

  """
  @spec generate() :: t

  def generate, do: bingenerate() |> encode()

  @doc ~S"""
  Generates a version 7 UUID in the binary format.

  ## Example

      Bookk.Ecto.UUIDv7.bingenerate()
      #> <<1, 142, 144, 216, 6, 232, 127, 159, 191, 215, 103, 48, 186, 152, 165, 27>>

  """
  @spec bingenerate() :: raw

  def bingenerate do
    time_ns = next_ascending()
    time_ms = div(time_ns, @ns_per_ms)

    clock_precision =
      (rem(time_ns, @ns_per_ms) * @possible_values)
      |> div(@ns_per_ms)

    <<_rand_a::2, rand_b::62>> = :crypto.strong_rand_bytes(8)

    <<
      time_ms::big-unsigned-48,
      @version::4,
      clock_precision::big-unsigned-@sub_ms_bits,
      @variant::2,
      rand_b::62
    >>
  end

  # Get an always-ascending unix nanosecond timestamp.
  #
  # We use `:atomics` to ensure this works with concurrent executions
  # without race conditions.
  #
  # See Postgres' version in C {https://github.com/postgres/postgres/blob/0e42d31b0b2273c376ce9de946b59d155fac589c/src/backend/utils/adt/uuid.c#L480}.
  defp next_ascending do
    timestamp_ref =
      #      get the atomic ref for the timestamp, initializing it if
      #    ↓ it doesn't exist yet
      with nil <- :persistent_term.get(__MODULE__, nil) do
        timestamp_ref = :atomics.new(1, signed: false)
        :ok = :persistent_term.put(__MODULE__, timestamp_ref)
        timestamp_ref
      end

    previous_ts = :atomics.get(timestamp_ref, 1)
    min_step_ts = previous_ts + @minimal_step_ns
    current_ts = System.system_time(:nanosecond)

    new_ts =
      #    if the current timestamp is not at least the minimal step
      #    nanoseconds greater than the previous step, then we make
      #  ↓ it so
      if current_ts > min_step_ts,
         do: current_ts,
         else: min_step_ts

    compare_exchange(timestamp_ref, previous_ts, new_ts)
  end

  defp compare_exchange(timestamp_ref, previous_ts, new_ts) do
    case :atomics.compare_exchange(timestamp_ref, 1, previous_ts, new_ts) do
      :ok -> new_ts
      #      ↑ if the new value was written, then we return it
      updated_ts -> compare_exchange(timestamp_ref, updated_ts, updated_ts + @minimal_step_ns)
      #             ↑ if the atomic value has changed in the meantime,
      #               we add the minimal step nanoseconds value to
      #               that and try again
    end
  end

  @doc ~S"""
  Extract the millisecond timestamp from the UUID.

  ## Example

      iex> Bookk.Ecto.UUIDv7.extract_timestamp("018ecb40-c457-73e6-a400-000398daddd9")
      1712807003223

  """
  @spec extract_timestamp(t | raw) :: integer

  def extract_timestamp(<<timestamp_ms::big-unsigned-48, @version::4, _::76>>), do: timestamp_ms
  def extract_timestamp(<<_::288>> = uuid), do: decode(uuid) |> extract_timestamp()

  @doc ~S"""
  Encode a raw UUID to the string representation.

  ## Example

      iex> Bookk.Ecto.UUIDv7.encode(<<1, 142, 144, 216, 6, 232, 127, 159, 191, 215, 103, 48, 186, 152, 165, 27>>)
      "018e90d8-06e8-7f9f-bfd7-6730ba98a51b"

  """
  @spec encode(raw) :: t

  def encode(
        <<a1::4, a2::4, a3::4, a4::4, a5::4, a6::4, a7::4, a8::4, b1::4, b2::4, b3::4, b4::4,
          c1::4, c2::4, c3::4, c4::4, d1::4, d2::4, d3::4, d4::4, e1::4, e2::4, e3::4, e4::4,
          e5::4, e6::4, e7::4, e8::4, e9::4, e10::4, e11::4, e12::4>>
      ) do
    <<e(a1), e(a2), e(a3), e(a4), e(a5), e(a6), e(a7), e(a8), ?-, e(b1), e(b2), e(b3), e(b4), ?-,
      e(c1), e(c2), e(c3), e(c4), ?-, e(d1), e(d2), e(d3), e(d4), ?-, e(e1), e(e2), e(e3), e(e4),
      e(e5), e(e6), e(e7), e(e8), e(e9), e(e10), e(e11), e(e12)>>
  end

  @compile {:inline, e: 1}

  defp e(0), do: ?0
  defp e(1), do: ?1
  defp e(2), do: ?2
  defp e(3), do: ?3
  defp e(4), do: ?4
  defp e(5), do: ?5
  defp e(6), do: ?6
  defp e(7), do: ?7
  defp e(8), do: ?8
  defp e(9), do: ?9
  defp e(10), do: ?a
  defp e(11), do: ?b
  defp e(12), do: ?c
  defp e(13), do: ?d
  defp e(14), do: ?e
  defp e(15), do: ?f

  @doc ~S"""
  Decode a string representation of a UUID to the raw binary version.

  ## Example

      iex> Bookk.Ecto.UUIDv7.decode("018e90d8-06e8-7f9f-bfd7-6730ba98a51b")
      <<1, 142, 144, 216, 6, 232, 127, 159, 191, 215, 103, 48, 186, 152, 165, 27>>

  """
  @spec decode(t) :: raw | :error

  def decode(
        <<a1, a2, a3, a4, a5, a6, a7, a8, ?-, b1, b2, b3, b4, ?-, c1, c2, c3, c4, ?-, d1, d2, d3,
          d4, ?-, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12>>
      ) do
    <<d(a1)::4, d(a2)::4, d(a3)::4, d(a4)::4, d(a5)::4, d(a6)::4, d(a7)::4, d(a8)::4, d(b1)::4,
      d(b2)::4, d(b3)::4, d(b4)::4, d(c1)::4, d(c2)::4, d(c3)::4, d(c4)::4, d(d1)::4, d(d2)::4,
      d(d3)::4, d(d4)::4, d(e1)::4, d(e2)::4, d(e3)::4, d(e4)::4, d(e5)::4, d(e6)::4, d(e7)::4,
      d(e8)::4, d(e9)::4, d(e10)::4, d(e11)::4, d(e12)::4>>
  catch
    :error -> :error
  end

  def decode(_), do: :error

  @compile {:inline, d: 1}

  defp d(?0), do: 0
  defp d(?1), do: 1
  defp d(?2), do: 2
  defp d(?3), do: 3
  defp d(?4), do: 4
  defp d(?5), do: 5
  defp d(?6), do: 6
  defp d(?7), do: 7
  defp d(?8), do: 8
  defp d(?9), do: 9
  defp d(?A), do: 10
  defp d(?B), do: 11
  defp d(?C), do: 12
  defp d(?D), do: 13
  defp d(?E), do: 14
  defp d(?F), do: 15
  defp d(?a), do: 10
  defp d(?b), do: 11
  defp d(?c), do: 12
  defp d(?d), do: 13
  defp d(?e), do: 14
  defp d(?f), do: 15
  defp d(_), do: throw(:error)

  @impl Ecto.Type
  def type, do: :uuid

  @impl Ecto.Type
  def autogenerate, do: generate()

  @doc ~S"""
  Casts either a string in the canonical, human-readable UUID format
  or a 16-byte binary to a UUID in its canonical, human-readable UUID
  format.

  If `uuid` is neither of these, `:error` will be returned.

  Since both binaries and strings are represent as binaries, this
  means some strings you may not expect are actually also valid UUIDs
  in their binary form and so will be casted into their string form.

  ## Examples

      iex> Bookk.Ecto.UUIDv7.cast(<<1, 141, 236, 237, 26, 200, 116, 82, 179, 112, 220, 56, 9, 179, 208, 93>>)
      {:ok, "018deced-1ac8-7452-b370-dc3809b3d05d"}

      iex> Bookk.Ecto.UUIDv7.cast("018deced-1ac8-7452-b370-dc3809b3d05d")
      {:ok, "018deced-1ac8-7452-b370-dc3809b3d05d"}

      iex> Bookk.Ecto.UUIDv7.cast("warehouse worker")
      {:ok, "77617265-686f-7573-6520-776f726b6572"}

  """
  @impl Ecto.Type
  @spec cast(t | raw | any) :: {:ok, t} | :error

  def cast(
        <<a1, a2, a3, a4, a5, a6, a7, a8, ?-, b1, b2, b3, b4, ?-, c1, c2, c3, c4, ?-, d1, d2,
          d3, d4, ?-, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12>>
      ) do
    <<c(a1), c(a2), c(a3), c(a4), c(a5), c(a6), c(a7), c(a8), ?-, c(b1), c(b2), c(b3), c(b4),
      ?-, c(c1), c(c2), c(c3), c(c4), ?-, c(d1), c(d2), c(d3), c(d4), ?-, c(e1), c(e2), c(e3),
      c(e4), c(e5), c(e6), c(e7), c(e8), c(e9), c(e10), c(e11), c(e12)>>
  catch
    :error -> :error
  else
    hex_uuid -> {:ok, hex_uuid}
  end

  def cast(<<_::128>> = raw_uuid), do: {:ok, encode(raw_uuid)}
  def cast(_), do: :error

  @doc ~S"""
  Same as `cast/1` but raises `Ecto.CastError` on invalid arguments.
  """
  @spec cast!(t | raw | any) :: t

  def cast!(uuid) do
    case cast(uuid) do
      {:ok, hex_uuid} -> hex_uuid
      :error -> raise Ecto.CastError, type: __MODULE__, value: uuid
    end
  end

  @compile {:inline, c: 1}

  defp c(?0), do: ?0
  defp c(?1), do: ?1
  defp c(?2), do: ?2
  defp c(?3), do: ?3
  defp c(?4), do: ?4
  defp c(?5), do: ?5
  defp c(?6), do: ?6
  defp c(?7), do: ?7
  defp c(?8), do: ?8
  defp c(?9), do: ?9
  defp c(?A), do: ?a
  defp c(?B), do: ?b
  defp c(?C), do: ?c
  defp c(?D), do: ?d
  defp c(?E), do: ?e
  defp c(?F), do: ?f
  defp c(?a), do: ?a
  defp c(?b), do: ?b
  defp c(?c), do: ?c
  defp c(?d), do: ?d
  defp c(?e), do: ?e
  defp c(?f), do: ?f
  defp c(_), do: throw(:error)

  @doc ~S"""
  Converts a string representing a UUID into a raw binary.
  """
  @impl Ecto.Type
  @spec dump(uuid_string :: t | any) :: {:ok, raw} | :error

  def dump(uuid_string)

  def dump(uuid_string) do
    case decode(uuid_string) do
      :error -> :error
      raw_uuid -> {:ok, raw_uuid}
    end
  end

  @doc ~S"""
  Same as `dump/1` but raises `Ecto.ArgumentError` on invalid
  arguments.
  """
  @spec dump!(t | any) :: raw

  def dump!(uuid) do
    with :error <- decode(uuid) do
      raise ArgumentError, "cannot dump given UUID to binary: #{inspect(uuid)}"
    end
  end

  @doc ~S"""
  Converts a binary UUID into a string.
  """
  @impl Ecto.Type
  @spec load(raw | any) :: {:ok, t} | :error

  def load(<<_::128>> = raw_uuid), do: {:ok, encode(raw_uuid)}

  def load(<<_::64, ?-, _::32, ?-, _::32, ?-, _::32, ?-, _::96>> = string) do
    raise ArgumentError,
          "trying to load string UUID as UUID: #{inspect(string)}. " <>
            "Maybe you wanted to declare :uuid as your database field?"
  end

  def load(_), do: :error

  @doc ~S"""
  Same as `load/1` but raises `Ecto.ArgumentError` on invalid
  arguments.
  """
  @spec load!(raw | any) :: t

  def load!(value) do
    case load(value) do
      {:ok, hex_uuid} -> hex_uuid
      :error -> raise ArgumentError, "cannot load given binary as UUID: #{inspect(value)}"
    end
  end
end
