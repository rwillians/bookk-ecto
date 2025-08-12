defmodule Bookk.Ecto.Account do
  @moduledoc ~S"""
  Ecto model for a Ledger's Account.
  """

  use Ecto.Schema

  import Ecto.Changeset

  @type t() :: %Bookk.Ecto.Account{
          id: String.t(),
          ledger_id: String.t(),
          name: String.t(),
          class: String.t(),
          groups: [String.t(), ...],
          balance: Decimal.t(),
          meta: map(),
          inserted_at: DateTime.t(),
          updated_at: DateTime.t()
        }

  @primary_key {:id, :string, []}
  schema "bookk_accounts" do
    field :ledger_id, :string
    field :name, :string
    field :class, :string
    field :groups, {:array, :string}
    field :balance, :decimal
    field :meta, :map, default: %{}
    field :inserted_at, :utc_datetime_usec
    field :updated_at, :utc_datetime_usec
  end

  @doc false
  @spec changeset(record, params) :: Ecto.Changeset.t()
        when record: t(),
             params: map()

  @all_fields [:id, :ledger_id, :name, :class, :groups, :balance, :meta, :inserted_at, :updated_at]
  def changeset(%Bookk.Ecto.Account{} = record, params) do
    record
    |> cast(params, @all_fields)
    |> validate_required(@all_fields)
    |> validate_length(:id, min: 1, max: 255)
    |> validate_length(:ledger_id, min: 1, max: 48)
    |> validate_length(:name, min: 1, max: 207)
  end
end
