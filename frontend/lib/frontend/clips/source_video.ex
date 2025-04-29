defmodule Frontend.Clips.SourceVideo do
  use Frontend.Clips.Schema

  schema "source_videos" do
    field :filepath,         :string
    field :duration_seconds, :float
    field :fps,              :float
    field :width,            :integer
    field :height,           :integer
    field :published_date,   :date
    field :title,            :string
    field :ingest_state,     :string, default: "new"
    field :last_error,       :string
    field :retry_count,      :integer, default: 0
    field :original_url,     :string
    timestamps()
  end
end
