defmodule Frontend.Clips do
  @moduledoc """
  The **Clips** context — everything about fetching, reviewing and annotating
  clips.

  ### Responsibilities

  * **Queue helpers** – return batches of `pending_review` clips that the
    LiveView keeps in memory (`next_pending_review_clips/2`).

  * **Event sourcing** – write `clip_events` rows for every UI action and flip
    `reviewed_at` so the clip leaves the queue.

  * **Composite actions** – *merge*, *group*, *split* all perform more than one
    DB mutation but still return the “next job” in a single round-trip.

  * **Sibling browsing** – `for_source_video/4` returns pages of other clips
    from the same source video (used by the new merge/group-by-ID mode).

  All public helpers either:

    * return `{:ok, {next_clip_or_nil, context}}` (single-row helpers) **or**
    * return `{next_clip_or_nil, context}` inside a DB transaction
      (composite helpers).

  `context` makes downstream telemetry / job-queueing simpler.
  """

  # -------------------------------------------------------------------------
  # Imports / aliases
  # -------------------------------------------------------------------------

  import Ecto.Query, warn: false
  alias Ecto.Query, as: Q

  alias Frontend.Repo
  alias Frontend.Clips.{Clip, ClipEvent}

  # -------------------------------------------------------------------------
  # Constants (UI → DB action map)
  # -------------------------------------------------------------------------

  @action_map %{
    "approve" => "selected_approve",
    "skip"    => "selected_skip",
    "archive" => "selected_archive",
    "undo"    => "selected_undo",
    "group"   => "selected_group_source",
    "split"   => "selected_split"
  }

  # -------------------------------------------------------------------------
  # Internal helpers
  # -------------------------------------------------------------------------

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

  # -------------------------------------------------------------------------
  # Public helpers added for the new UI
  # -------------------------------------------------------------------------

  @doc """
  Return **one** clip (with `:source_video` and `:clip_artifacts` preloaded)
  or raise if it doesn’t exist.

  Used by *ReviewLive* when the reviewer types an explicit ID in merge/group-
  by-ID mode.
  """
  @spec get_clip!(integer) :: Clip.t()
  def get_clip!(id) when is_integer(id), do: load_clip_with_assocs(id)

  @doc """
  Paged list of **other** clips that belong to the same `source_video`.

      iex> for_source_video(42, 777, 2, 24)
      # second page (offset 24) of clips whose `source_video_id` is 42,
      # excluding clip 777 itself, 24 rows max – preloaded for thumbnail grid.

  * `sv_id`       – the `source_video.id` that all clips must share
  * `exclude_id`  – the *current* clip (will be omitted from the result)
  * `page`        – 1-based page index
  * `per`         – page size (defaults to 24 thumbnails)

  Results are ordered by `id ASC` to make pagination deterministic even when
  background workers update timestamps.
  """
  def for_source_video_with_sprites(source_video_id, exclude_id, page, page_size) do
    Clip
    |> join(:inner, [c], ca in assoc(c, :clip_artifacts),
         on: ca.artifact_type == "sprite_sheet")
    |> where([c, _ca],
         c.source_video_id == ^source_video_id and c.id != ^exclude_id)
    |> distinct([c, _ca], c.id)
    |> order_by([c, _ca], asc: c.id)
    |> offset(^((page - 1) * page_size))
    |> limit(^page_size)
    |> preload([c, ca], clip_artifacts: ca)
    |> Repo.all()
  end

  # -------------------------------------------------------------------------
  # Public API – fetch helpers
  # -------------------------------------------------------------------------

  @doc """
  Return up to `limit` clips still awaiting review, excluding any IDs in
  `exclude_ids`.

  Clips are ordered by *id* to remain stable even if background workers update
  timestamps.
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

  @doc "Legacy single-row wrapper kept for tests / scripts."
  def next_pending_review_clip do
    next_pending_review_clips(1) |> List.first()
  end

  # -------------------------------------------------------------------------
  # Public API – single-row action (approve / skip / archive / undo)
  # -------------------------------------------------------------------------

  @doc """
  Log `ui_action` for `clip`, mark it reviewed (or *un-review* on undo),
  and return the next clip to review (or `nil`) in one SQL round-trip.

  Uses raw SQL `WITH … FOR UPDATE SKIP LOCKED` to avoid queue collisions.
  """
  def select_clip_and_fetch_next(%Clip{id: clip_id}, ui_action) do
    reviewer_id = "admin"                          # TODO: pull from auth
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

  @doc "Convenience helper for ad-hoc writes outside the batched path."
  def log_clip_action!(clip_id, action, reviewer_id) do
    %ClipEvent{}
    |> ClipEvent.changeset(%{
      clip_id:     clip_id,
      action:      action,
      reviewer_id: reviewer_id
    })
    |> Repo.insert!()
  end

  # -------------------------------------------------------------------------
  # Public API – composite helpers (merge / group / split)
  # -------------------------------------------------------------------------

  @doc """
  Handle a **merge** request between *prev ⇠ current* clips.

  1. Log both sides of the merge
  2. Mark both clips reviewed & store processing metadata
  3. Fetch the next `pending_review` clip

  Returns `{next_clip_or_nil, context}`.
  """
  def request_merge_and_fetch_next(%Clip{id: prev_id},
                                   %Clip{id: curr_id}) do
    reviewer_id = "admin"
    now         = DateTime.utc_now()

    Repo.transaction(fn ->
      # ① log both sides
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

      # ② mark reviewed & attach metadata
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

      # ③ fetch next job
      next_id =
        Q.from(c in Clip,
          where: c.ingest_state == "pending_review" and is_nil(c.reviewed_at),
          order_by: c.id,
          limit: 1,
          lock: "FOR UPDATE SKIP LOCKED",
          select: c.id)
        |> Repo.one()

      next_clip = if next_id, do: load_clip_with_assocs(next_id)

      {next_clip,
       %{clip_id_source: curr_id, clip_id_target: prev_id, action: "merge"}}
    end)
  end

  @doc """
  Handle a **group** request between *prev ⇠ current* clips.

  1. Log target & source events
  2. Mark *current* clip reviewed
  3. Fetch the next job
  """
  def request_group_and_fetch_next(%Clip{id: prev_id},
                                   %Clip{id: curr_id}) do
    reviewer_id = "admin"
    now         = DateTime.utc_now()

    Repo.transaction(fn ->
      # ① target side
      %ClipEvent{}
      |> ClipEvent.changeset(%{
        clip_id:     prev_id,
        action:      "selected_group_target",
        reviewer_id: reviewer_id
      })
      |> Repo.insert!()

      # ② source side
      %ClipEvent{}
      |> ClipEvent.changeset(%{
        clip_id:     curr_id,
        action:      "selected_group_source",
        reviewer_id: reviewer_id,
        event_data:  %{"group_with_clip_id" => prev_id}
      })
      |> Repo.insert!()

      # ③ mark current reviewed
      Repo.update_all(
        from(c in Clip, where: c.id == ^curr_id),
        set: [reviewed_at: now]
      )

      # ④ fetch next job
      next_id =
        Q.from(c in Clip,
          where: c.ingest_state == "pending_review" and is_nil(c.reviewed_at),
          order_by: c.id,
          limit: 1,
          lock: "FOR UPDATE SKIP LOCKED",
          select: c.id)
        |> Repo.one()

      next_clip = if next_id, do: load_clip_with_assocs(next_id)

      {next_clip,
       %{clip_id_source: curr_id, clip_id_target: prev_id, action: "group"}}
    end)
  end

  @doc """
  Handle a **split** request on `clip` at `frame_num`.

  1. Log `selected_split` with `split_at_frame`
  2. Mark the clip reviewed
  3. Fetch the next job
  """
  def request_split_and_fetch_next(%Clip{id: clip_id}, frame_num)
      when is_integer(frame_num) do
    reviewer_id = "admin"
    now         = DateTime.utc_now()

    Repo.transaction(fn ->
      # ① record event
      %ClipEvent{}
      |> ClipEvent.changeset(%{
        clip_id:     clip_id,
        action:      "selected_split",
        reviewer_id: reviewer_id,
        event_data:  %{"split_at_frame" => frame_num}
      })
      |> Repo.insert!()

      # ② mark reviewed and attach split frame to processing_metadata
      Repo.update_all(
        from(c in Clip, where: c.id == ^clip_id),
        set: [
          reviewed_at:         now,
          processing_metadata: %{"split_at_frame" => frame_num}
        ]
      )

      # ③ fetch next clip
      next_id =
        Q.from(c in Clip,
          where: c.ingest_state == "pending_review" and is_nil(c.reviewed_at),
          order_by: c.id,
          limit: 1,
          lock: "FOR UPDATE SKIP LOCKED",
          select: c.id)
        |> Repo.one()

      next_clip = if next_id, do: load_clip_with_assocs(next_id)

      {next_clip,
       %{clip_id: clip_id, action: "split", frame: frame_num}}
    end)
  end
end
