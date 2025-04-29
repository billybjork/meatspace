defmodule Frontend.Clips.ClipArtifact do
  use Frontend.Clips.Schema

  schema "clip_artifacts" do
    field :artifact_type, :string
    field :strategy,      :string
    field :tag,           :string
    field :s3_key,        :string
    field :metadata,      :map

    belongs_to :clip, Frontend.Clips.Clip
    timestamps(inserted_at: :created_at, updated_at: :updated_at)
  end
end
