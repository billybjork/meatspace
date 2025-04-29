defmodule Frontend.Clips.ClipEvent do
  use Frontend.Clips.Schema

  schema "clip_events" do
    field :action,      :string
    field :reviewer_id, :string
    field :event_data,  :map

    belongs_to :clip, Frontend.Clips.Clip

    timestamps(inserted_at: :created_at, updated_at: :updated_at)
  end

  @doc false
  def changeset(event, attrs) do
    event
    |> cast(attrs, [:clip_id, :action, :reviewer_id, :event_data])
    |> validate_required([:clip_id, :action])
  end
end
