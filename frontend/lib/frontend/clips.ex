defmodule Frontend.Clips do
  @moduledoc """
  The Clips context: review, query, and annotate clips.
  """

  import Ecto.Query, warn: false
  alias Frontend.Repo

  alias Frontend.Clips.{Clip, ClipEvent}

  @doc """
  Gets a single clip by ID, preloading related data.
  Raises if not found.
  """
  def get_clip!(id) do
    Repo.get!(Clip, id)
    |> Repo.preload([:source_video, :clip_artifacts])
  end

  @doc """
  Fetch the next clip pending review, or nil if none found.
  """
  def next_pending_review_clip do
    Clip
    |> where([c], c.ingest_state == "pending_review")
    |> order_by([c], [asc: c.updated_at, asc: c.id])
    |> preload([:source_video, :clip_artifacts])
    |> limit(1)
    |> Repo.one()
  end

  @doc """
  Logs an action taken on a clip, and returns the next clip.
  """
  def select_clip_and_fetch_next(%Clip{id: clip_id} = _clip, action) do
    Repo.transaction(fn ->
      reviewer = "admin" # or get from session later
      log_clip_action!(clip_id, action, reviewer)

      next = next_pending_review_clip()
      undo = %{clip_id: clip_id, action: action}
      {next, undo}
    end)
  end

  @doc """
  Records a clip event.
  """
  def log_clip_action!(clip_id, action, reviewer) do
    %ClipEvent{}
    |> ClipEvent.changeset(%{clip_id: clip_id, action: action, reviewer_id: reviewer})
    |> Repo.insert!()
  end
end
