defmodule BookkEcto.MixProject do
  use Mix.Project

  @version "0.1.0"
  @github "https://github.com/rwillians/bookk-ecto"

  @description """
  Ecto adapter for persisting Bookk's ledger state.
  """

  def project do
    [
      app: :bookk_ecto,
      version: @version,
      description: @description,
      source_url: @github,
      homepage_url: @github,
      elixir: ">= 1.14.0",
      elixirc_paths: elixirc_paths(Mix.env()),
      elixirc_options: [debug_info: Mix.env() == :dev],
      build_embedded: Mix.env() not in [:dev, :test],
      aliases: aliases(),
      package: package(),
      docs: [
        main: "readme",
        logo: "assets/hex-logo.png",
        source_ref: "v#{@version}",
        source_url: @github,
        canonical: "http://hexdocs.pm/bookk_ecto/",
        extras: ["README.md", "LICENSE"]
      ],
      deps: deps(),
      dialyzer: [
        plt_add_apps: [:mix],
        plt_add_deps: :apps_direct
      ]
    ]
  end

  def aliases do
    [
      #
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:bookk, github: "rwillians/bookk", ref: "50dca06"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false, optional: true},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false, optional: true},
      {:ecto, "~> 3.13"},
      {:ecto_ulid_next, "~> 1.0"},
      {:ex_doc, "~> 0.38", only: [:dev, :docs], runtime: false}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp package do
    [
      files: ~w(lib mix.exs .formatter.exs README.md LICENSE),
      maintainers: ["Rafael Willians"],
      contributors: ["Rafael Willians"],
      licenses: ["MIT"],
      links: %{
        GitHub: @github,
        Changelog: "#{@github}/releases"
      }
    ]
  end
end
