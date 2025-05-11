defmodule Frontend.Clips.Embedding do
  @moduledoc """
  Ecto schema for the `embeddings` table, storing vector embeddings for each clip.

  Columns:
  - id: primary key
  - clip_id: belongs_to association to `clips`
  - embedding: `vector` (Pgvector.Ecto.Vector)
  - model_name: text
  - model_version: text (nullable)
  - generation_strategy: text
  - generated_at: timestamp with time zone (inserted_at)
  - embedding_dim: integer (nullable)
  """

  use Ecto.Schema
  import Ecto.Changeset

  alias Frontend.Clips.Clip
  alias Pgvector.Ecto.Vector

  @primary_key {:id, :id, autogenerate: true}
  @timestamps_opts [type: :utc_datetime_usec]

  schema "embeddings" do
    # Associate embedding back to its clip
    belongs_to :clip, Clip, type: :integer

    # Store embeddings as a Postgres vector
    field :embedding, Vector    # uses Pgvector.Ecto.Vector :contentReference[oaicite:0]{index=0}

    field :model_name, :string
    field :model_version, :string
    field :generation_strategy, :string
    field :embedding_dim, :integer

    # Use generated_at column for inserted_at; no updated_at
    timestamps(inserted_at: :generated_at, updated_at: false)
  end

  @required_fields ~w(clip_id model_name generation_strategy embedding)a
  @optional_fields ~w(model_version embedding_dim generated_at)a

  @doc false
  def changeset(embedding_struct, attrs) do
    embedding_struct
    |> cast(attrs, @required_fields ++ @optional_fields)
    |> validate_required(@required_fields)
    |> foreign_key_constraint(:clip_id)
  end
end
