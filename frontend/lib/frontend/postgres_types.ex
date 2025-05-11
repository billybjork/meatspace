Postgrex.Types.define(
  Frontend.PostgresTypes,
  [Pgvector.Extensions.Vector] ++ Ecto.Adapters.Postgres.extensions(),
  []
)
