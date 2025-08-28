defmodule Bookk.Ecto.AccountEntry do
  @moduledoc ~S"""
  Ecto schema for an Account Entry, which is a log of Operations applied to a
  Ledger's Account.

  ## Related

  - `Bookk.Ledger`;
  - `Bookk.Account`;
  - `Bookk.Operation`.
  """

  use Ecto.Schema

  import Ecto.Changeset

  @type t() :: %Bookk.Ecto.AccountEntry{
          id: Bookk.Ecto.UUIDv7.t(),
          account_id: String.t(),
          transaction_id: String.t(),
          delta_amount: Decimal.t(),
          balance_after: Decimal.t(),
          inserted_at: DateTime.t()
        }

  @primary_key {:id, Bookk.Ecto.UUIDv7, autogenerate: true}
  schema "bookk_account_entries" do
    field :account_id, :string
    field :transaction_id, Bookk.Ecto.UUIDv7
    field :delta_amount, :decimal
    field :balance_after, :decimal
    field :inserted_at, :utc_datetime_usec
  end

  @doc false
  @spec changeset(record, params) :: Ecto.Changeset.t()
        when record: t(),
             params: map()

  @fields [:account_id, :transaction_id, :delta_amount, :balance_after, :inserted_at]
  def changeset(%Bookk.Ecto.AccountEntry{} = record, params) do
    record
    |> cast(params, @fields)
    |> validate_required(@fields)
    |> validate_length(:account_id, min: 1, max: 255)
    |> validate_length(:transaction_id, min: 1, max: 36)
  end
end
