defmodule Frontend.Repo.Migrations.MakeClipTimestampsUsec do
  use Ecto.Migration

  def change do
    alter table(:clips) do
      modify :reviewed_at,   :utc_datetime_usec
      modify :keyframed_at,  :utc_datetime_usec
      modify :embedded_at,   :utc_datetime_usec
      modify :action_committed_at, :utc_datetime_usec
    end
  end
end
