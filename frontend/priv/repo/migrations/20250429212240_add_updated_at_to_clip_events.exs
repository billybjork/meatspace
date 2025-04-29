# priv/repo/migrations/<timestamp>_add_updated_at_to_clip_events.exs
defmodule Frontend.Repo.Migrations.AddUpdatedAtToClipEvents do
  use Ecto.Migration

  def change do
    alter table(:clip_events) do
      add :updated_at, :utc_datetime_usec,
          null: false,
          default: fragment("timezone('utc', now())")
          # or simply: default: fragment("now()")
    end

    # keep timestamps in sync for existing rows
    execute("UPDATE clip_events SET updated_at = created_at")
  end
end
