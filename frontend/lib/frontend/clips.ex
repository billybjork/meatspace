defmodule Frontend.Clips do
  @moduledoc """
  The **Clips** context — fetch, review and annotate clips.

  * `next_pending_review_clips/2` — bulk, duplicate-free fetch that the
    LiveView uses to keep its in-memory queue full.
  * `select_clip_and_fetch_next/2` — writes the `clip_events` row, flips
    the `reviewed_at` flag **and** returns the id of the next clip,
    guarded by `FOR UPDATE SKIP LOCKED` so no two reviewers collide.

  All three steps happen in **one** SQL round-trip.
  """

  import Ecto.Query, warn: false
  alias Frontend.Repo
  alias Frontend.Clips.{Clip, ClipEvent}

  @action_map %{
    "approve"  => "selected_approve",
    "skip"     => "selected_skip",
    "archive"  => "selected_archive",
    "undo"     => "selected_undo"
  }

  # ------------------------------------------------------------------
  # helpers
  # ------------------------------------------------------------------

  @spec load_clip_with_assocs(integer) :: Clip.t()
  defp load_clip_with_assocs(id) do
    from(c in Clip,
      where: c.id == ^id,
      left_join: sv in assoc(c, :source_video),
      left_join: ca in assoc(c, :clip_artifacts),
      preload: [source_video: sv, clip_artifacts: ca]
    )
    |> Repo.one!()
  end

  # ------------------------------------------------------------------
  # public API
  # ------------------------------------------------------------------

  @doc """
  Fetch **`limit`** clips still awaiting review, omitting any ids in
  **`exclude_ids`** (used to avoid duplicates already held in memory).

  The list is ordered solely by **`id`** to remain stable even if
  background jobs touch `updated_at`.
  """
  def next_pending_review_clips(limit, exclude_ids \\ []) when is_integer(limit) do
    Clip
    |> where([c], c.ingest_state == "pending_review" and is_nil(c.reviewed_at))
    |> where([c], c.id not in ^exclude_ids)
    |> order_by([c], asc: c.id)
    |> limit(^limit)
    |> preload([:source_video, :clip_artifacts])
    |> Repo.all()
  end

  @doc """
  Legacy single-row wrapper so any old call-sites keep compiling.
  """
  def next_pending_review_clip do
    next_pending_review_clips(1) |> List.first()
  end

  @doc """
  Log **`action`** for **`clip`** and return the next clip to review.

  * Inserts a row into **`clip_events`**
  * Sets / clears `reviewed_at`
  * Streams back the *next* clip id **with a row-level lock**

  Returns `{:ok, {next_clip_or_nil, ctx}}`.
  The LiveView ignores the result but you might want it elsewhere.
  """
  def select_clip_and_fetch_next(%Clip{id: clip_id}, ui_action) do
    reviewer_id = "admin"        # TODO: pull from session / auth
    db_action   = Map.get(@action_map, ui_action, ui_action)

    {:ok, %{rows: rows}} =
      Repo.query("""
        WITH ins AS (
          INSERT INTO clip_events (action, clip_id, reviewer_id)
          VALUES ($1, $2, $3)
        ), upd AS (
          UPDATE clips
          SET    reviewed_at = CASE WHEN $1 = 'selected_undo'
                                    THEN NULL
                                    ELSE NOW()
                               END
          WHERE  id = $2
        )
        SELECT id
        FROM   clips
        WHERE  ingest_state = 'pending_review'
          AND  reviewed_at IS NULL
        ORDER  BY id
        LIMIT  1
        FOR UPDATE SKIP LOCKED;
      """, [db_action, clip_id, reviewer_id])

    next_clip =
      case rows do
        [[id]] -> load_clip_with_assocs(id)
        _      -> nil
      end

    {:ok, {next_clip, %{clip_id: clip_id, action: db_action}}}
  end

  @doc """
  Convenience helper for one-off writes outside the batched path above.
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

  def request_merge_and_fetch_next(%Clip{id: prev_id},
                                  %Clip{id: curr_id}) do
      reviewer_id = "admin"
      now         = DateTime.utc_now()

      Repo.transaction(fn ->
        # ①  log both sides of the merge request -------------------------------
        Repo.insert_all(ClipEvent, [
          %{clip_id: prev_id, action: "selected_merge_target",
            reviewer_id: reviewer_id, inserted_at: now, updated_at: now},
          %{clip_id: curr_id, action: "selected_merge_source",
            reviewer_id: reviewer_id, inserted_at: now, updated_at: now}
        ])

        # ②  just mark them as *reviewed* and record the metadata --------------
        Repo.update_all(
          from(c in Clip, where: c.id == ^prev_id),
          set: [reviewed_at: now,
                processing_metadata: %{"merge_source_clip_id" => curr_id}]
        )

        Repo.update_all(
          from(c in Clip, where: c.id == ^curr_id),
          set: [reviewed_at: now,
                processing_metadata: %{"merge_target_clip_id" => prev_id}]
        )

        # ③  grab the next clip, same as the other path ------------------------
        next_id =
          Repo.one("""
            SELECT id
            FROM   clips
            WHERE  ingest_state = 'pending_review'
              AND  reviewed_at IS NULL
            ORDER  BY id
            LIMIT  1
            FOR UPDATE SKIP LOCKED
        """)

      next_clip = if next_id, do: load_clip_with_assocs(next_id)
      {next_clip,
      %{clip_id_source: curr_id, clip_id_target: prev_id, action: "merge"}}
    end)
  end

end
