defmodule Bookk.Ecto do
  @moduledoc ~S"""
  Ecto persistense layer adapter for `Bookk`.

      defmodule MyApp.Bookkeeping do
        use Bookk.Ecto

        alias Bookk.AccountClass, as: Class
        alias Bookk.AccountHead, as: Account

        @impl Bookk.ChartOfAccounts
        def class("A"), do: %Class{id: "A", parent_id: nil, name: "Assets", natural_balance: :debit}
        def class("CA"), do: %Class{id: "CA", parent_id: "A", name: "Current Assets", natural_balance: :debit}

        @impl Bookk.ChartOfAccounts
        def account(:cash), do: %Account{name: "cash", class: class("CA")}

        @impl Bookk.ChartOfAccounts
        def ledger_id(:main), do: "main"
      end

  ## Options

  - `otp_app`: the name of your OTP app. This option is required if you want to
    configure bookk ecto's settings in your config files;
  - `accounts_table`: the name of the table where the accounts' balance state
    should be persisted. Defaults to "bookk_accounts";
  - `account_entries_table`: the name of the table where the accounts balances'
    change log should be persisted. Defaults to "bookk_account_entries";
  - `chart_of_accounts`: your chart of accounts module, in case you prefer to
    have it in a separate module. The absence of this option implies that your
    bookkeeping module should also implement `Bookk.ChartOfAccounts`.

        defmodule MyApp.Bookkeeping do
          use Bookk.Ecto,
            accounts_table: "bookkeeping_accounts",
            account_entries_table: "bookkeeping_account_entries",
            chart_of_accounts: MyApp.Bookkeeping.ChartOfAccounts
        end

  Alternativelly, these options can be set in your config files where
  the configuration key is the name of your bookkeeping module:

      config :my_app, MyApp.Bookkeeping,
        accounts_table: "bookk_accounts",
        account_entries_table: "bookk_account_entries",
        chart_of_accounts: MyApp.Bookkeeping.ChartOfAccounts

      defmodule MyApp.Bookkeeping do
        use Bookk.Ecto, otp_app: :my_app
      end

  **Make sure these configs are available at compiletime!**

  You can also override your configurations by providing the options you want
  to override to `Bookk.Ecto`:

      config :my_app, MyApp.Bookkeeping,
        accounts_table: "bookkeeping_v1__accounts"

      defmodule MyApp.BookkeepingV1 do
        use Bookk.Ecto,
          otp_app: :my_app,
          account_entries_table: "bookkeeping_v1__account_entries"
      end

      defmodule MyApp.BookkeepingV2 do
        use Bookk.Ecto,
          otp_app: :my_app,
          accounts_table: "bookk_accounts",
          account_entries_table: "bookk_acount_entries"
      end

  ## Migration

  These are the tables and indexes that should be created:

      defmodule MyApp.Repo.Migrations.CreateBookkTables do
        use Ecto.Migration

        def change do
          create table(:bookk_accounts, primary_key: false) do
            add :id, :string, size: 255, primary_key: true
            add :ledger_id, :string, size: 48, null: false
            add :name, :string, size: 207, null: false
            add :class, :string, size: 3, null: false
            add :groups, {:array, :string}, size: 3, null: false
            add :balance, :decimal, precision: 14, scale: 2, null: false
            add :meta, :map, null: false
            add :inserted_at, :utc_datetime_usec, null: false
            add :updated_at, :utc_datetime_usec, null: false
          end

          create index(:bookk_accounts, [:ledger_id])
          create index(:bookk_accounts, [:groups], using: :gin)

          create table(:bookk_acount_entries, primary_key: false) do
            add :id, :uuid, primary_key: true
            add :account_id, :string, size: 255, null: false
            add :transaction_id, :uuid, null: false
            add :delta_amount, :decimal, precision: 14, scale: 2, null: false
            add :balance_after, :decimal, precision: 14, scale: 2, null: false
            add :inserted_at, :utc_datetime_usec, null: false
          end

          create index(:bookk_acount_entries, [:account_id, "inserted_at ASC"])
          create index(:bookk_acount_entries, [:transaction_id])
        end
      end

  """

  import Bookk.InterledgerEntry, only: [to_journal_entries: 1]
  import Bookk.JournalEntry, only: [to_operations: 1]
  import Bookk.Operation, only: [to_delta_amount: 1]
  import Ecto, only: [put_meta: 2]
  import Ecto.Changeset, only: [put_change: 3]

  alias __MODULE__, as: Config
  alias Bookk.AccountClass
  alias Bookk.Ecto.Account
  alias Bookk.Ecto.AccountEntry
  alias Bookk.Ecto.UUIDv7
  alias Bookk.InterledgerEntry
  alias Bookk.Operation, as: Op
  alias Bookk.Options
  alias Ecto.Multi

  @options otp_app: [type: :atom],
           accounts_table: [type: :string, required: true, default: "bookk_accounts"],
           account_entries_table: [type: :string, required: true, default: "bookk_account_entries"],
           chart_of_accounts: [type: :module, required: true]

  @type t() :: %Config{
          otp_app: atom() | nil,
          accounts_table: String.t(),
          account_entries_table: String.t(),
          chart_of_accounts: module()
        }

  defstruct otp_app: nil,
            accounts_table: nil,
            account_entries_table: nil,
            chart_of_accounts: nil

  @doc false
  defmacro __using__(opts_from_use) do
    otp_app = Keyword.get(opts_from_use, :otp_app)

    quote location: :keep do
      @config [chart_of_accounts: __MODULE__]
              |> Keyword.merge(if(is_atom(unquote(otp_app)), do: Application.compile_env(unquote(otp_app), __MODULE__, []), else: []))
              |> Keyword.merge(unquote(opts_from_use))
              |> unquote(__MODULE__).parse_config()

      if @config.chart_of_accounts == __MODULE__ do
        use Bookk.ChartOfAccounts
      end

      @doc ~S"""
      Introspection function that returns the module's configs.

          MyApp.Bookkeeping.__config__()
          #> %Bookk.Ecto{...}

      """
      @spec __config__() :: unquote(__MODULE__).t()

      def __config__, do: @config

      @doc ~S"""
      Introspection function that returns the value of a specific config.

          MyApp.Bookkeeping.__config__(:accounts_table)
          #> "bookk_accounts"

      If you try to read a config that doesn't exist, it will raise an
      `ArgumentError`.

      The available configs are:
      - `:otp_app`;
      - `:accounts_table`;
      - `:account_entries_table`; and
      - `:chart_of_accounts`;
      """
      @spec __config__(key :: atom()) :: term()

      def __config__(key) when is_atom(key), do: Map.fetch!(@config, key)

      @doc ~S"""
      Given a class id, it returns all the groups to which the account class
      belongs.

            MyApp.Bookkeeping.account_groups("CA")
            #> ["CA", "A"]

      """
      @spec account_groups(class_id :: String.t()) :: [String.t(), ...]

      def account_groups(class_id),
        do: unquote(__MODULE__).account_groups(class_id, @config)

      @doc ~S"""
      Takes the balance of an account after posting changes to it.

          {:ok, multi_result} = MyApp.Bookkeeping.post(interledger_entry)

          ledger_id = MyApp.Bookkeeping.ledger_id(:acme)
          account = MyApp.Bookkeeping.account(:cash)
          account_id = MyApp.Bookkeeping.account_id(ledger_id, account)

          MyApp.Bookkeeping.balance_after(multi_result, account_id)
          #> %Decimal{...}

      Alternatively, you can provide the ledger code and the account
      code directly to `balance_after/2` instead of the account id:

        ledger_code = :acme
        account_code = :cash

        MyApp.Bookkeeping.balance_after(multi_result, {ledger_code, account_code})
        #> %Decimal{...}

      If the multi result object doesn't contain the account's balance,
      it will return zero.
      """
      @spec balance_after(multi_result, account_id | {ledger_code, account_code}) :: balance
            when multi_result: map(),
                 account_id: String.t(),
                 ledger_code: term(),
                 account_code: term(),
                 balance: Decimal.t()

      def balance_after(%{} = multi_result, <<_, _::binary>> = account_id),
        do: unquote(__MODULE__).balance_after(multi_result, account_id, @config)

      @doc ~S"""
      Returns a queriable that can be used either for defining schema
      relations ships or when composing a query.

      ## Examples

      Using it in a schema:

          import MyApp.Bookkeeping, only: [bookk_schema: 1]

          schema "wallets" do
            # ...
            belongs_to :cash_account, bookk_schema(:account)
            # ...
          end

          schema "transactions" do
            # ...
            has_many :account_entries, bookk_schema(:account_entry)
            # ...
          end

      Using it in a query:

          import MyApp.Bookkeeping, only: [bookk_schema: 1]

          from a in bookk_schema(:account)
          from a in bookk_schema(:account_entry)

      """
      defmacro bookk_schema(:account) do
        quote do
          {unquote(@config.accounts_table), Bookk.Ecto.Account}
        end
      end

      defmacro bookk_schema(:account_entry) do
        quote do
          {unquote(@config.account_entries_table), Bookk.Ecto.AccountEntry}
        end
      end

      @doc ~S"""
      Fetches bookk's transaction id from the given multi result
      object.
      """
      @spec fetch_transaction_id!(multi_result) :: transaction_id
            when multi_result: map(),
                 transaction_id: String.t()

      def fetch_transaction_id!(%{} = multi_result),
        do: unquote(__MODULE__).fetch_transaction_id!(multi_result)

      @doc ~S"""
      Appends all the database operations related to posting the side effects of
      the given interledger entry to the database into the given `Ecto.Multi`.
      The transaction id is populated into the multi's result object under the
      key :bookk_transaction_id.

          {:ok, result} =
            Ecto.Multi.new()
            |> MyApp.Bookkeeping.post(interledger_entry)
            |> MyApp.Repo.transaction()

          result.bookk_transaction_id
          #> "01K2F8W26ACNQJA9R34Z43QP8S"

      """
      @spec post(Ecto.Multi.t(), Bookk.InterledgerEntry.t()) :: Ecto.Multi.t()

      def post(%Ecto.Multi{} = multi, %Bookk.InterledgerEntry{} = interledger_entry),
        do: unquote(__MODULE__).post(multi, interledger_entry, @config)
    end
  end

  @doc false
  def account_groups(<<_, _::binary>> = class_id, %Config{} = config) do
    class = apply(config.chart_of_accounts, :class, [class_id])

    case class do
      nil -> raise(ArgumentError, "Account class #{class_id} doesn't exist in #{inspect(config.chart_of_accounts)}")
      %AccountClass{parent_id: nil} -> [class.id]
      %AccountClass{parent_id: <<parent_id::binary>>} -> [class.id | account_groups(parent_id, config)]
    end
  end

  @doc false
  def balance_after(%{} = multi_result, <<_, _::binary>> = account_id, _) do
    %Decimal{} =
      balance_after =
      multi_result
      |> Map.get(to_existing_atom_safe("bookk_#{account_id}"), %{})
      |> Map.get(:balance, Decimal.new(0))

    balance_after
  end

  def balance_after(%{} = multi_result, {ledger_code, account_code}, %Config{} = config) do
    %Config{chart_of_accounts: coa} = config

    ledger_id = apply(coa, :ledger_id, [ledger_code])
    account_head = apply(coa, :account, [account_code])
    account_id = apply(coa, :account_id, [ledger_id, account_head])

    balance_after(multi_result, account_id, config)
  end

  @doc false
  def parse_config(opts) do
    {parsed, errors} = Options.parse(@options, opts)

    with {key, message} <- List.first(errors),
         do: raise(ArgumentError, "Invalid option :#{key} provideded to #{__MODULE__}: #{message}")

    struct!(Config, parsed)
  end

  @doc false
  def post(%Multi{} = multi, %InterledgerEntry{} = entry, %Config{} = config) do
    ledger_ops =
      for {ledger_id, journal_entry} <- to_journal_entries(entry),
          op <- to_operations(journal_entry),
          do: {ledger_id, op}

    tx = %{
      id: UUIDv7.generate(),
      timestamp: DateTime.utc_now()
    }

    multi
    |> Multi.put(:bookk_transaction_id, tx.id)
    |> multi_reduce(ledger_ops, &post_op(&1, &2, tx, config))
  end

  @doc false
  def fetch_transaction_id!(%{} = multi_result), do: Map.fetch!(multi_result, :bookk_transaction_id)

  #
  #   PRIVATE
  #

  defp multi_reduce(multi, [], _), do: multi
  defp multi_reduce(multi, [head | tail], fun), do: multi_reduce(fun.(multi, head), tail, fun)

  defp post_op(multi, {ledger_id, %Op{} = op}, tx, %Config{} = config) do
    account_id = apply(config.chart_of_accounts, :account_id, [ledger_id, op.account_head])

    payload = %{
      transaction_id: tx.id,
      ledger_id: ledger_id,
      account_id: account_id,
      account_name: op.account_head.name,
      account_class_id: op.account_head.class.id,
      account_groups: account_groups(op.account_head.class.id, config),
      account_meta: op.account_head.meta,
      delta_amount: to_delta_amount(op),
      accounts_table: config.accounts_table,
      account_entries_table: config.account_entries_table,
      account_op_name: to_existing_atom_safe("bookk_#{account_id}"),
      account_entry_op_name: to_existing_atom_safe("bookk_#{account_id}_entry"),
      timestamp: tx.timestamp
    }

    multi
    |> upsert_account(payload)
    |> insert_account_entry(payload)
  end

  defp to_existing_atom_safe(<<_, _::binary>> = str) do
    String.to_existing_atom(str)
  rescue
    _ -> String.to_atom(str)
  end

  defp upsert_account(multi, payload) do
    changeset =
      %Account{}
      |> put_meta(source: payload.accounts_table)
      |> Account.changeset(%{
        id: payload.account_id,
        ledger_id: payload.ledger_id,
        name: payload.account_name,
        class: payload.account_class_id,
        groups: payload.account_groups,
        balance: payload.delta_amount,
        meta: payload.account_meta,
        inserted_at: payload.timestamp,
        updated_at: payload.timestamp
      })

    upsert_opts = [
      conflict_target: [:id],
      on_conflict: [
        inc: [balance: payload.delta_amount],
        set: [meta: payload.account_meta, updated_at: payload.timestamp]
      ],
      returning: [:balance]
    ]

    Multi.insert(multi, payload.account_op_name, changeset, upsert_opts)
  end

  defp insert_account_entry(multi, payload) do
    changeset =
      %AccountEntry{}
      |> put_meta(source: payload.account_entries_table)
      |> AccountEntry.changeset(%{
        account_id: payload.account_id,
        transaction_id: payload.transaction_id,
        delta_amount: payload.delta_amount,
        balance_after: Decimal.new(0),
        #                          ↑ placeholder, will be replaced in `put_balance_after/3`
        inserted_at: payload.timestamp
      })

    Multi.insert(multi, payload.account_entry_op_name, &put_balance_after(changeset, payload.account_op_name, &1))
  end

  defp put_balance_after(changeset, account_op_name, multi_ctx) do
    account = Map.fetch!(multi_ctx, account_op_name)
    put_change(changeset, :balance_after, account.balance)
  end
end
