defmodule Frontend.Clips do
  @moduledoc """
  The **Clips** context — fetch, review and annotate clips.

  * `next_pending_review_clips/2` — bulk, duplicate-free fetch that the
    LiveView uses to keep its in-memory queue full.
  * `select_clip_and_fetch_next/2` — writes the `clip_events` row, flips
    the `reviewed_at` flag **and** returns the id of the next clip,
    guarded by `FOR UPDATE SKIP LOCKED` so no two reviewers collide.
  * `request_merge_and_fetch_next/2` — logs a merge of two clips, marks
    both as reviewed with metadata, then returns the next clip.

  All three steps happen inside a single DB transaction or round-trip.
  """

  # bring in Ecto.Query and alias it as Q for our raw-DSL queries
  import Ecto.Query, warn: false
  alias Ecto.Query, as: Q

  alias Frontend.Repo
  alias Frontend.Clips.{Clip, ClipEvent}

  # map UI action names to the DB event names we record
  @action_map %{
    "approve" => "selected_approve",
    "skip"    => "selected_skip",
    "archive" => "selected_archive",
    "undo"    => "selected_undo",
    "group"   => "selected_group_source",
    "split"   => "selected_split"
  }

  # ------------------------------------------------------------------
  # internal helpers
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
  Fetch `limit` clips still awaiting review, omitting any ids in
  `exclude_ids` (used to avoid duplicates already held in memory).

  Ordered by `id` to remain stable even if background jobs touch timestamps.
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
  Legacy single-row wrapper.
  """
  def next_pending_review_clip do
    next_pending_review_clips(1) |> List.first()
  end

  @doc """
  Log `ui_action` for `clip`, mark it reviewed (or clear on undo), and
  return the next clip to review. Uses raw SQL WITH … FOR UPDATE SKIP LOCKED.
  """
  def select_clip_and_fetch_next(%Clip{id: clip_id}, ui_action) do
    reviewer_id = "admin"                  # TODO: pull from session/auth
    db_action   = Map.get(@action_map, ui_action, ui_action)

    {:ok, %{rows: rows}} =
      Repo.query(
        """
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
        """,
        [db_action, clip_id, reviewer_id]
      )

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

  @doc """
  Handle a merge of two clips:
    1. log `selected_merge_target` on the previous clip
       and `selected_merge_source` on the current clip
    2. mark both as reviewed and attach processing_metadata
    3. fetch the next pending_review clip with FOR UPDATE SKIP LOCKED

  Returns `{next_clip_or_nil, ctx}` in the same shape as select_clip_and_fetch_next.
  """
  def request_merge_and_fetch_next(%Clip{id: prev_id}, %Clip{id: curr_id}) do
    reviewer_id = "admin"
    now         = DateTime.utc_now()

    Repo.transaction(fn ->
      # ① log both sides of the merge request
      %ClipEvent{}
      |> ClipEvent.changeset(%{
        clip_id:     prev_id,
        action:      "selected_merge_target",
        reviewer_id: reviewer_id
      })
      |> Repo.insert!()

      %ClipEvent{}
      |> ClipEvent.changeset(%{
        clip_id:     curr_id,
        action:      "selected_merge_source",
        reviewer_id: reviewer_id
      })
      |> Repo.insert!()

      # ② mark both as reviewed & record merge metadata
      Repo.update_all(
        from(c in Clip, where: c.id == ^prev_id),
        set: [
          reviewed_at:        now,
          processing_metadata: %{"merge_source_clip_id" => curr_id}
        ]
      )

      Repo.update_all(
        from(c in Clip, where: c.id == ^curr_id),
        set: [
          reviewed_at:        now,
          processing_metadata: %{"merge_target_clip_id" => prev_id}
        ]
      )

      # ③ fetch the next pending_review clip via Ecto query
      next_id =
        Q.from(c in Clip,
          where: c.ingest_state == "pending_review" and is_nil(c.reviewed_at),
          order_by: c.id,
          limit: 1,
          lock: "FOR UPDATE SKIP LOCKED",
          select: c.id
        )
        |> Repo.one()

      next_clip = if next_id, do: load_clip_with_assocs(next_id)

      {next_clip,
       %{clip_id_source: curr_id, clip_id_target: prev_id, action: "merge"}}
    end)
  end

  @doc """
  Log a *group* request between two clips (prev ⇢ curr) and return the
  next clip to review, just like `request_merge_and_fetch_next/2`.
  """
  def request_group_and_fetch_next(%Clip{id: prev_id}, %Clip{id: curr_id}) do
    reviewer_id = "admin"
    now         = DateTime.utc_now()

    Repo.transaction(fn ->
      # ① log the *target* side (pure marker – lets you add per‑group stats later)
      %ClipEvent{}
      |> ClipEvent.changeset(%{
        clip_id:     prev_id,
        action:      "selected_group_target",
        reviewer_id: reviewer_id
      })
      |> Repo.insert!()

      # ② log the *source* side and keep the relation in event_data
      %ClipEvent{}
      |> ClipEvent.changeset(%{
        clip_id:     curr_id,
        action:      "selected_group_source",
        reviewer_id: reviewer_id,
        event_data:  %{"group_with_clip_id" => prev_id}
      })
      |> Repo.insert!()

      # ③ mark **current** clip as reviewed so the UI can advance
      Repo.update_all(
        from(c in Clip, where: c.id == ^curr_id),
        set: [reviewed_at: now]
      )

      # ④ fetch the next job exactly the same way as merge does
      next_id =
        Q.from(c in Clip,
          where: c.ingest_state == "pending_review" and is_nil(c.reviewed_at),
          order_by: c.id,
          limit: 1,
          lock: "FOR UPDATE SKIP LOCKED",
          select: c.id
        )
        |> Repo.one()

      next_clip = if next_id, do: load_clip_with_assocs(next_id)

      {next_clip,
       %{clip_id_source: curr_id,
         clip_id_target: prev_id,
         action: "group"}}
    end)
  end

  @doc """
  Handle a split request for a clip:
    1. log `selected_split` on the clip with `split_at_frame` in event_data
    2. mark the clip as reviewed
    3. fetch the next pending_review clip with FOR UPDATE SKIP LOCKED

  Returns `{next_clip_or_nil, ctx}`.
  """
  def request_split_and_fetch_next(%Clip{id: clip_id}, frame_number) when is_integer(frame_number) do
    reviewer_id = "admin" # TODO: pull from session/auth
    db_action = "selected_split"
    # frame_number is guaranteed to be an integer here
    event_data_payload = %{"split_at_frame" => frame_number}

    # Using raw SQL WITH for atomicity of event logging, clip update, and next clip fetch
    {:ok, %{rows: rows}} =
      Repo.query(
        """
        WITH ins AS (
          INSERT INTO clip_events (action, clip_id, reviewer_id, event_data, created_at, updated_at)
          VALUES ($1, $2, $3, $4, NOW(), NOW())
        ), upd AS (
          UPDATE clips
          SET    reviewed_at = NOW(), updated_at = NOW()
          WHERE  id = $2
        )
        SELECT id
        FROM   clips
        WHERE  ingest_state = 'pending_review'
          AND  reviewed_at IS NULL
        ORDER  BY id
        LIMIT  1
        FOR UPDATE SKIP LOCKED;
        """,
        [db_action, clip_id, reviewer_id, event_data_payload]
      )

    next_clip =
      case rows do
        [[id]] -> load_clip_with_assocs(id)
        _      -> nil
      end

    {:ok, {next_clip, %{clip_id: clip_id, action: db_action, split_frame: frame_number}}}
  end
end
