defmodule Frontend.Clips.SourceVideo do
  use Frontend.Clips.Schema

  @type t() :: %__MODULE__{
    id: integer(),
    filepath: String.t() | nil,
    duration_seconds: float() | nil,
    fps: float() | nil,
    width: integer() | nil,
    height: integer() | nil,
    published_date: Date.t() | nil,
    title: String.t() | nil,
    ingest_state: String.t(),
    last_error: String.t() | nil,
    retry_count: integer(),
    original_url: String.t() | nil,
    # Ecto timestamps
    created_at: NaiveDateTime.t(),
    updated_at: NaiveDateTime.t()
  }

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
    timestamps(inserted_at: :created_at, updated_at: :updated_at)
  end
end
