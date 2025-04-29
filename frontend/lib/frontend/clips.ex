defmodule Frontend.Clips do
  @moduledoc """
  Public API for clip review & ingestion.
  """

  import Ecto.Query
  alias Frontend.Repo
  alias Frontend.Clips.{Clip, ClipArtifact, ClipEvent}

  # -- 2.1 List / fetch helpers ---------------------------------------------

  @doc "Returns the *oldest* pending-review clip plus its latest sprite sheet."
  def next_pending_review_clip(after_clip \\ nil) do
    sprite_sub =
      from a in ClipArtifact,
        where: a.artifact_type == "sprite_sheet",
        select: %{id: max(a.id)},  # latest per clip
        group_by: a.clip_id

    base_q =
      from c in Clip,
        where: c.ingest_state == "pending_review",
        join: sv in assoc(c, :source_video),
        left_join: amax in subquery(sprite_sub),
        on: amax.id == fragment("?::integer", amax.id),
        left_join: a in ClipArtifact,
        on: a.id == amax.id,
        order_by: [asc: c.updated_at, asc: c.id],
        limit: 1,
        preload: [source_video: sv, clip_artifacts: a]

    base_q =
      case after_clip do
        nil ->
          base_q

        %Clip{id: id, updated_at: ts} ->
          from c in base_q,
            where:
              c.updated_at > ^ts or
                (c.updated_at == ^ts and c.id > ^id)
      end

    Repo.one(base_q)
  end

  @doc "Record a user action (approve/skip/archive/undo) **without mutating clip state**."
  def log_clip_action!(%Clip{id: clip_id}, action, reviewer \\ "admin") do
    %ClipEvent{}
    |> ClipEvent.changeset(%{clip_id: clip_id, action: action, reviewer_id: reviewer})
    |> Repo.insert!()
  end

  # -- 2.2 Atomic transaction used by LiveView ------------------------------

  @doc """
  Logs the action and returns `{next_clip, undo_ctx}`.

  *Exactly* mirrors the control-flow you prototyped in Python :contentReference[oaicite:0]{index=0}&#8203;:contentReference[oaicite:1]{index=1}.
  """
  def select_clip_and_fetch_next(clip, action, reviewer \\ "admin") do
    Repo.transaction(fn ->
      log_clip_action!(clip, "selected_#{action}", reviewer)

      # Decide next clip *inside* the transaction to avoid races.
      next_clip =
        case action do
          "undo"      -> clip
          _otherwise  -> next_pending_review_clip(clip)
        end

      undo_ctx =
        case action do
          "undo" -> nil
          _      -> %{clip_id: clip.id, action: action}
        end

      {next_clip, undo_ctx}
    end)
  end
end
