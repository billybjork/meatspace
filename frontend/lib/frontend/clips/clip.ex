defmodule Frontend.Clips.Clip do
  use Frontend.Clips.Schema

  schema "clips" do
    field :clip_filepath,        :string
    field :clip_identifier,      :string
    field :start_frame,          :integer
    field :end_frame,            :integer
    field :start_time_seconds,   :float
    field :end_time_seconds,     :float
    field :ingest_state,         :string,  default: "new"
    field :last_error,           :string
    field :retry_count,          :integer, default: 0
    field :reviewed_at,          :utc_datetime
    field :keyframed_at,         :utc_datetime
    field :embedded_at,          :utc_datetime
    field :processing_metadata,  :map
    field :grouped_with_clip_id, :integer
    field :action_committed_at,  :utc_datetime

    belongs_to :source_video, Frontend.Clips.SourceVideo
    has_many   :clip_artifacts, Frontend.Clips.ClipArtifact
    has_many   :clip_events,    Frontend.Clips.ClipEvent
    timestamps(inserted_at: :created_at, updated_at: :updated_at)
  end
end
