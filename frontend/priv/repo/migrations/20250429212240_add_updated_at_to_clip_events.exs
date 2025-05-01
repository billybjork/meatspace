# priv/repo/migrations/<timestamp>_add_updated_at_to_clip_events.exs
defmodule Frontend.Repo.Migrations.AddUpdatedAtToClipEvents do
  use Ecto.Migration

  def change do
    alter table(:clip_events) do
      add_if_not_exists :updated_at, :utc_datetime_usec
    end
  end

end
