defmodule Bookk.Ecto do
  @moduledoc ~S"""
  Ecto persistense layer adapter for `Bookk`.

      defmodule MyApp.Bookkeeping do
        use Bookk.Ecto, repo: Pag3.Repo

        @impl Bookk.ChartOfAccounts
        def ledger(:main), do: "main"

        @impl Bookk.ChartOfAccounts
        def class("A"), do: %Class{id: "A", parent_id: nil, name: "Assets", natural_balance: :debit}
        def class("CA"), do: %Class{id: "CA", parent_id: "A", name: "Current Assets", natural_balance: :debit}

        @impl Bookk.ChartOfAccounts
        def account(:cash), do: %Account{name: "cash/CA", class: class("CA")}
      end

  ## Options

  - `otp_app`: the name of your OTP app. This option is required if you want to
    set options in your config files;
  - `repo`: required, the Ecto repository to which the bookkeeping state should
    be persisted;
  - `accounts_table`: the name of the table where the accounts' balance state
    should be persisted. Defaults to "bookk_accounts";
  - `account_entries_table`: the name of the table where the accounts balances'
    change log should be persisted. Defaults to "bookk_account_entries";
  - `chart_of_accounts`: your chart of accounts module, in case you prefer to
    have it in a separate module. The absence of this option implies that your
    bookkeeping module should also implement `Bookk.ChartOfAccounts`.

        defmodule MyApp.Bookkeeping do
          use Bookk.Ecto,
            repo: MyApp.Repo,
            accounts_table: "bookkeeping_accounts",
            account_entries_table: "bookkeeping_account_entries",
            chart_of_accounts: MyApp.Bookkeeping.ChartOfAccounts
        end

  Alternativelly, these options can be set in your config files where
  the configuration key is the name of your bookkeeping module:

      config :my_app, MyApp.Bookkeeping,
        repo: MyApp.Repo,
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
        repo: MyApp.Repo

      defmodule MyApp.BookkeepingV1 do
        use Bookk.Ecto,
          otp_app: :my_app,
          accounts_table: "bookkeeping_v1__accounts",
          account_entries_table: "bookkeeping_v1__account_entries"
      end

      defmodule MyApp.BookkeepingV2 do
        use Bookk.Ecto,
          otp_app: :my_app,
          accounts_table: "bookkeeping_v2__accounts",
          account_entries_table: "bookkeeping_v2__account_entries"
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
  alias Bookk.InterledgerEntry
  alias Bookk.Operation, as: Op
  alias Bookk.Options
  alias Ecto.Multi
  alias Ecto.ULID

  @options otp_app: [type: :atom],
           repo: [type: :module, required: true],
           accounts_table: [type: :string, required: true, default: "bookk_accounts"],
           account_entries_table: [type: :string, required: true, default: "bookk_account_entries"],
           chart_of_accounts: [type: :module, required: true]

  @type t() :: %Config{
          otp_app: atom() | nil,
          repo: Ecto.Repo.t(),
          accounts_table: String.t(),
          account_entries_table: String.t(),
          chart_of_accounts: module()
        }

  defstruct otp_app: nil,
            repo: nil,
            accounts_table: nil,
            account_entries_table: nil,
            chart_of_accounts: nil

  @doc false
  defmacro __using__(opts_from_use) do
    otp_app = Keyword.get(opts_from_use, :otp_app)

    quote do
      @config [chart_of_accounts: __MODULE__]
              |> Keyword.merge(if(is_atom(unquote(otp_app)), do: Application.compile_env(otp_app, __MODULE__), else: []))
              |> Keyword.merge(unquote(opts_from_use))
              |> unquote(__MODULE__).parse_config(opts)

      if @config.chart_of_accounts == __MODULE__ do
        use Bookk.ChartOfAccounts

        alias Bookk.AccountClass, as: Class
        alias Bookk.AccountHead, as: Account
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
      - `:repo`;
      - `:accounts_table`;
      - `:account_entries_table`; and
      - `:chart_of_accounts`;
      """
      @spec __config__(key :: atom()) :: term()

      def __config__(key) when is_atom(key), do: Map.fetch!(@config, key)

      @doc ~S"""
      Returns the configured accounts / account entries table name and model.

          from a in bookk_table(:accounts),
            where: a.updated_at >= ^timestamp

          from a in bookk_table(:account_entries),
            where: a.created_at >= ^timestamp

      """
      defmacro bookk_table(:accounts) do
        quote do
          {unquote(@config.accounts_table), Bookk.Ecto.Account}
        end
      end

      defmacro bookk_table(:account_entries) do
        quote do
          {unquote(@config.account_entries_table), Bookk.Ecto.AccountEntry}
        end
      end

      @doc ~S"""
      Posts an interledger entry to the bookkeeping pesisted state in the
      configured Ecto repository, returning the id of the transaction.

          {:ok, result} = MyApp.Bookkeeping.post(interledger_entry)

          result.bookk_transaction_id
          #> "01K2F8W26ACNQJA9R34Z43QP8S"

      """
      @spec post(Bookk.InterledgerEntry.t()) ::
              {:ok, result}
              | {:error, multi_op_name, reason, result}
            when result: map(),
                 multi_op_name: atom(),
                 reason: term()

      def post(%Bookk.InterledgerEntry{} = interledger_entry) do
        Ecto.Multi.new()
        |> unquote(__MODULE__).post(interledger_entry, @config)
        |> @config.repo.transaction()
      end

      @doc ~S"""
      Same as `post/1` but appends all the database operations to the given
      `Ecto.Multi`. The transaction id is populated into the multi's result
      object under the key :bookk_transaction_id.

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

      @doc ~S"""
      Given a class id, it returns all the groups to which the account class
      belongs.

            MyApp.Bookkeeping.resolve_account_groups("CA")
            #> ["CA", "A"]

      """
      @spec resolve_account_groups(class_id :: String.t()) :: [String.t(), ...]

      def resolve_account_groups(class_id),
        do: unquote(__MODULE__).resolve_account_groups(class_id, @config)
    end
  end

  @doc false
  def resolve_account_groups(<<class_id::binary>>, %Config{} = config) do
    class = apply(config.chart_of_accounts, :class, [class_id])

    case class do
      nil -> raise(ArgumentError, "Account class #{class_id} doesn't exist in #{inspect(config.chart_of_accounts)}")
      %AccountClass{parent_id: nil} -> [class.id]
      %AccountClass{parent_id: <<parent_id::binary>>} -> [class.id | resolve_account_groups(parent_id, config)]
    end
  end

  @doc false
  def parse_config(opts) do
    {parsed, errors} = Options.parse(@options, opts)

    first_error_message =
      with {key, message} <- List.first(errors),
           do: "Invalid option :#{key} provideded to #{__MODULE__}: #{message}"

    unless is_nil(first_error_message) do
      raise ArgumentError,
        message: first_error_message
    end

    struct!(Config, parsed)
  end

  @doc false
  def post(%Multi{} = multi, %InterledgerEntry{} = entry, %Config{} = config) do
    ledger_ops =
      for {ledger, journal_entry} <- to_journal_entries(entry),
          op <- to_operations(journal_entry),
          do: {ledger, op}

    tx = %{
      id: ULID.generate(),
      timestamp: DateTime.utc_now()
    }

    multi
    |> Multi.put(:bookk_transaction_id, tx.id)
    |> multi_reduce(ledger_ops, &post_op(&2, &1, tx, config))
  end

  #
  #   PRIVATE
  #

  defp multi_reduce(multi, [], _), do: multi
  defp multi_reduce(multi, [head | tail], fun), do: multi_reduce(fun.(multi, head), tail, fun)

  defp post_op(multi, {ledger_id, %Op{} = op}, tx, %Config{} = config) do
    account_id = "#{ledger_id}:#{op.account_head.name}"

    payload = %{
      transaction_id: tx.id,
      ledger_id: ledger_id,
      account_id: account_id,
      account_name: op.account_head.name,
      account_class_id: op.account_head.class.id,
      account_groups: resolve_account_groups(op.account_head.class.id, config),
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
