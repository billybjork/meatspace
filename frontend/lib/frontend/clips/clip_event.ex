defmodule Frontend.Clips.ClipEvent do
  use Frontend.Clips.Schema

  schema "clip_events" do
    field :action,      :string
    field :reviewer_id, :string
    field :event_data,  :map

    belongs_to :clip, Frontend.Clips.Clip
    timestamps(inserted_at: :created_at)  # matches your SQL default
  end
end
