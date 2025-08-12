defmodule Bookk.Options do
  @moduledoc """
  A module for working with predefined options meant for internal use only! This
  API can — and most likely will — break compatibility.
  """

  @doc ~S"""
  Parses a keyword list of data according to a given schema.

      iex> options_schema = [
      iex>   otp_app: [type: :atom, required: true],
      iex>   accounts_table: [type: :string, default: "bookk_accounts"]
      iex> ]
      iex>
      iex> options = [
      iex>   otp_app: :my_app
      iex> ]
      iex>
      iex> Bookk.Options.parse(options_schema, options)
      {[otp_app: :my_app, accounts_table: "bookk_accounts"], []}

      iex> options_schema = [
      iex>   otp_app: [type: :atom, required: true],
      iex>   accounts_table: [type: :string, default: "bookk_accounts"]
      iex> ]
      iex>
      iex> options = [
      iex>   otp_app: "my_app",
      iex>   accounts_table: :bookk_accounts
      iex> ]
      iex>
      iex> Bookk.Options.parse(options_schema, options)
      {[], [
        otp_app: "expected an atom, got \"my_app\"",
        accounts_table: "expected a string, got :bookk_accounts"
      ]}

  """
  @spec parse(schema, data) :: {parsed, errors}
        when schema: [{atom(), [spec, ...]}],
             spec:
               {:default, any()}
               | {:required, boolean()}
               | {:type, type},
             type:
               :atom
               | {:fun, arity :: non_neg_integer()}
               | :module
               | :string
               | {:tuple, [type, ...]}
               | {:either, [type, ...]},
             data: [{field :: atom(), value :: term()}],
             parsed: [{field :: atom(), value :: term()}],
             errors: [{field :: atom(), message :: String.t()}]

  def parse(schema, data) when is_list(schema) and is_list(data) do
    results = Enum.map(schema, parse_field(data))
    {successes, failures} = Keyword.split_with(results, &(elem(&1, 0) == :ok))

    parsed = Enum.map(successes, &elem(&1, 1))
    errors = Enum.map(failures, &elem(&1, 1))

    {parsed, errors}
  end

  defp normalize_spec(spec) do
    [
      # the order matters
      default: Keyword.get(spec, :default, nil),
      required: Keyword.get(spec, :required, false),
      type: Keyword.fetch!(spec, :type)
    ]
  end

  defp parse_field(data), do: &parse_field(elem(&1, 0), normalize_spec(elem(&1, 1)), Keyword.get(data, elem(&1, 0)))

  defp parse_field(key, spec, value) do
    case test(spec, value) do
      {:ok, value} -> {:ok, {key, value}}
      {:error, reason} -> {:error, {key, reason}}
    end
  end

  defp test([], value), do: {:ok, value}
  defp test([{:default, default} | tail], nil), do: test(tail, default)
  defp test([{:default, _} | tail], value), do: test(tail, value)
  defp test([{:required, true} | _], nil), do: {:error, "is required"}
  defp test([{:required, true} | _], ""), do: {:error, "is required"}
  defp test([{:required, _} | tail], value), do: test(tail, value)

  defp test([{:type, type} | tail], value) do
    case tt(type, value) do
      :ok -> test(tail, value)
      {:error, message} -> {:error, message}
    end
  end

  # [t]est [t]ype
  defp tt(_, nil), do: :ok
  defp tt(:atom, value) when is_atom(value), do: :ok
  defp tt({:fun, arity}, value) when is_function(value, arity), do: :ok
  defp tt(:module, value) when is_atom(value), do: :ok
  defp tt(:string, <<_::binary>>), do: :ok

  defp tt({:tuple, shape} = type, value) when is_tuple(value) do
    valid? =
      shape
      |> Enum.to_list()
      |> Enum.with_index()
      |> Enum.all?(fn {type, index} -> tt(type, elem(value, index)) == :ok end)

    if valid?,
      do: :ok,
      else: {:error, "expected #{st(type)}, got #{inspect(value)}"}
  end

  defp tt({:either, [head | tail]}, value) do
    case {tt(head, value), tail} do
      {:ok, _} -> {:ok, value}
      {{:error, _}, [_ | _]} -> tt({:either, tail}, value)
      {{:error, _} = error, []} -> error
    end
  end

  defp tt(type, value), do: {:error, "expected #{st(type)}, got #{inspect(value)}"}

  # [s]tringify [t]ype
  defp st(:atom), do: "an atom"
  defp st({:fun, arity}), do: "a function with arity #{arity}"
  defp st(:module), do: "a module name"
  defp st(:string), do: "a string"
  defp st({:tuple, shape}), do: "a tuple with shape #{inspect(shape)}"
  defp st({:either, types}), do: "expected one of #{st({:list, or: types})}"

  defp st({:list, [{connector, list}]}) when connector in [:and, :or] and is_list(list) do
    [trailing | rest] = :lists.reverse(Enum.map(list, &st/1))
    leading = Enum.join(:lists.reverse(rest), ", ")
    "#{leading} #{connector} #{trailing}"
  end
end
