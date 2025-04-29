defmodule Frontend.Clips do
  @moduledoc """
  The **Clips** context — fetch, review and annotate clips.

  ### Performance note (2025-04-29)
  `select_clip_and_fetch_next/2` now completes the three critical steps

  1. Insert a `clip_events` row
  2. Mark (or un-mark) the current clip as reviewed
  3. Return the *next* pending-review clip id

  in **one SQL round-trip** using a `WITH` CTE.  This removes two network
  RTTs when the database is remote (e.g. Railway) and noticeably cuts
  click-to-next-clip latency.
  """

  import Ecto.Query, warn: false
  alias Frontend.Repo
  alias Frontend.Clips.{Clip, ClipEvent}

  # --------------------------------------------------------------------
  # 🛠  Helper – single query that preloads associations
  # --------------------------------------------------------------------
  @spec load_clip_with_assocs(integer()) :: Clip.t()
  defp load_clip_with_assocs(id) do
    from(c in Clip,
      where: c.id == ^id,
      left_join: sv in assoc(c, :source_video),
      left_join: ca in assoc(c, :clip_artifacts),
      preload: [source_video: sv, clip_artifacts: ca]
    )
    |> Repo.one!()
  end

  # --------------------------------------------------------------------
  # Public API
  # --------------------------------------------------------------------

  @doc """
  Fetch a single clip **by id**, eagerly pre-loading the associations we
  always need in the review UI.
  """
  def get_clip!(id) do
    Clip
    |> Repo.get!(id)
    |> Repo.preload([:source_video, :clip_artifacts])
  end

  @doc """
  Return *the* next clip that still needs review, or `nil` if the queue
  is empty.
  """
  def next_pending_review_clip do
    Clip
    |> where([c], c.ingest_state == "pending_review" and is_nil(c.reviewed_at))
    |> order_by([c], asc: c.id)
    |> preload([:source_video, :clip_artifacts])
    |> limit(1)
    |> Repo.one()
  end

  @doc """
  Log an **`action`** taken on `clip`, mutate its `reviewed_at` flag as
  appropriate, and stream back the next clip to review **in one shot**.

  Returns `{:ok, {next_clip_or_nil, undo_ctx}}`, where `undo_ctx` is used
  by the LiveView to render the "undo" toast.
  """
  def select_clip_and_fetch_next(%Clip{id: clip_id}, action) do
    reviewer_id = "admin"  # TODO: pull from session / auth layer

    {:ok, %{rows: rows}} =
      Repo.query(
        """
        WITH ins AS (
          INSERT INTO clip_events (action, clip_id, reviewer_id)
          VALUES ($1, $2, $3)
        ), upd AS (
          UPDATE clips
          SET reviewed_at = CASE WHEN $1 = 'undo' THEN NULL ELSE now() END
          WHERE id = $2
        )
        SELECT id
        FROM   clips
        WHERE  ingest_state = 'pending_review'
          AND  reviewed_at IS NULL
        ORDER  BY id
        LIMIT  1;
        """,
        [action, clip_id, reviewer_id]
      )

    next_clip =
      case rows do
        [[next_id]] -> load_clip_with_assocs(next_id)
        _ -> nil
      end

    undo_ctx = %{clip_id: clip_id, action: action}
    {:ok, {next_clip, undo_ctx}}
  end

  @doc """
  Convenience helper to create a `ClipEvent` in places *outside* the
  batched path above.
  """
  def log_clip_action!(clip_id, action, reviewer_id) do
    %ClipEvent{}
    |> ClipEvent.changeset(%{
      clip_id:     clip_id,
      action:      action,
      reviewer_id: reviewer_id
    })
    |> Repo.insert!()
  end
end
