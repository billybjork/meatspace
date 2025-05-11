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

  * **Sibling browsing** – `for_source_video_with_sprites/4` returns pages of other clips
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
  alias Frontend.Clips.{Clip, ClipEvent, Embedding}

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
  # Public API – review queue & actions
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

  @doc "Log `ui_action` for `clip`, mark it reviewed (or *un-review* on undo), and return the next clip to review in one SQL round-trip."
  def select_clip_and_fetch_next(%Clip{id: clip_id}, ui_action) do
    reviewer_id = "admin"  # TODO: pull from auth
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
    |> ClipEvent.changeset(%{clip_id: clip_id, action: action, reviewer_id: reviewer_id})
    |> Repo.insert!()
  end

  @doc "Handle a **merge** request between *prev ⇠ current* clips."
  def request_merge_and_fetch_next(%Clip{id: prev_id}, %Clip{id: curr_id}) do
    reviewer_id = "admin"
    now         = DateTime.utc_now()

    Repo.transaction(fn ->
      # log both sides
      %ClipEvent{}
      |> ClipEvent.changeset(%{clip_id: prev_id, action: "selected_merge_target", reviewer_id: reviewer_id})
      |> Repo.insert!()

      %ClipEvent{}
      |> ClipEvent.changeset(%{clip_id: curr_id, action: "selected_merge_source", reviewer_id: reviewer_id})
      |> Repo.insert!()

      # mark reviewed & attach metadata
      Repo.update_all(
        from(c in Clip, where: c.id == ^prev_id),
        set: [reviewed_at: now, processing_metadata: %{"merge_source_clip_id" => curr_id}]
      )

      Repo.update_all(
        from(c in Clip, where: c.id == ^curr_id),
        set: [reviewed_at: now, processing_metadata: %{"merge_target_clip_id" => prev_id}]
      )

      # fetch next job
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
      {next_clip, %{clip_id_source: curr_id, clip_id_target: prev_id, action: "merge"}}
    end)
  end

  @doc "Handle a **group** request between *prev ⇠ current* clips."
  def request_group_and_fetch_next(%Clip{id: prev_id}, %Clip{id: curr_id}) do
    reviewer_id = "admin"
    now         = DateTime.utc_now()

    Repo.transaction(fn ->
      # target side
      %ClipEvent{}
      |> ClipEvent.changeset(%{clip_id: prev_id, action: "selected_group_target", reviewer_id: reviewer_id})
      |> Repo.insert!()

      # source side
      %ClipEvent{}
      |> ClipEvent.changeset(%{clip_id: curr_id, action: "selected_group_source", reviewer_id: reviewer_id, event_data: %{"group_with_clip_id" => prev_id}})
      |> Repo.insert!()

      # mark reviewed
      Repo.update_all(
        from(c in Clip, where: c.id == ^curr_id),
        set: [reviewed_at: now]
      )

      # fetch next job
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
      {next_clip, %{clip_id_source: curr_id, clip_id_target: prev_id, action: "group"}}
    end)
  end

  @doc "Handle a **split** request on `clip` at `frame_num`."
  def request_split_and_fetch_next(%Clip{id: clip_id}, frame_num) when is_integer(frame_num) do
    reviewer_id = "admin"
    now         = DateTime.utc_now()

    Repo.transaction(fn ->
      # record event
      %ClipEvent{}
      |> ClipEvent.changeset(%{clip_id: clip_id, action: "selected_split", reviewer_id: reviewer_id, event_data: %{"split_at_frame" => frame_num}})
      |> Repo.insert!()

      # mark reviewed & attach metadata
      Repo.update_all(
        from(c in Clip, where: c.id == ^clip_id),
        set: [reviewed_at: now, processing_metadata: %{"split_at_frame" => frame_num}]
      )

      # fetch next clip
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
      {next_clip, %{clip_id: clip_id, action: "split", frame: frame_num}}
    end)
  end

  @doc """
  Return **one** clip (with :source_video and :clip_artifacts preloaded)
  or raise if it doesn’t exist.

  Used by *ReviewLive* when the reviewer types an explicit ID in merge/group-
  by-ID mode.
  """
  @spec get_clip!(integer) :: Clip.t()
  def get_clip!(id) when is_integer(id), do: load_clip_with_assocs(id)

  @doc """
  Paged list of **other** clips that belong to the same source_video.

      iex> for_source_video(42, 777, 2, 24)
      # second page (offset 24) of clips whose source_video_id is 42,
      # excluding clip 777 itself, 24 rows max – preloaded for thumbnail grid.

  * sv_id       – the source_video.id that all clips must share
  * exclude_id  – the *current* clip (will be omitted from the result)
  * page        – 1-based page index
  * per         – page size (defaults to 24 thumbnails)

  Results are ordered by id ASC to make pagination deterministic even when
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
  # Public API – sibling browsing
  # -------------------------------------------------------------------------

  @doc "All available model names, generation strategies, and source videos for embedded clips"
  def embedded_filter_opts do
    model_names =
      Embedding
      |> where([e], not is_nil(e.generation_strategy))
      |> distinct([e], e.model_name)
      |> order_by([e], asc: e.model_name)
      |> select([e], e.model_name)
      |> Repo.all()

    gen_strats =
      Embedding
      |> distinct([e], e.generation_strategy)
      |> order_by([e], asc: e.generation_strategy)
      |> select([e], e.generation_strategy)
      |> Repo.all()

    source_videos =
      from(c in Clip,
        where: c.ingest_state == "embedded",
        join: sv in assoc(c, :source_video),
        distinct: sv.id,
        order_by: sv.title,
        select: {sv.id, sv.title}
      )
      |> Repo.all()

    %{model_names: model_names, generation_strategies: gen_strats, source_videos: source_videos}
  end

  @doc "Pick one random clip in state ‘embedded’, respecting optional filters"
  def random_embedded_clip(%{model_name: m, generation_strategy: g, source_video_id: sv}) do
    base =
      Embedding
      |> join(
           :inner, [e], c in Clip,
           on: c.id == e.clip_id and c.ingest_state == "embedded"
         )

    base =
      if m do
        from [e, c] in base, where: e.model_name == ^m
      else
        base
      end

    base =
      if g do
        from [e, c] in base, where: e.generation_strategy == ^g
      else
        base
      end

    base =
      if sv do
        from [e, c] in base, where: c.source_video_id == ^sv
      else
        base
      end

    base
    |> order_by(fragment("RANDOM()"))
    |> limit(1)
    |> select([_e, c], c)
    |> Repo.one()
    |> case do
         nil  -> nil
         clip -> Repo.preload(clip, :source_video)
       end
  end

  @doc """
  Given a main clip and the active filters, return page `page` of its neighbors,
  ordered by vector similarity (<=>), ascending or descending.
  """
  def similar_clips(main_clip_id, %{model_name: m, generation_strategy: g}, sort_asc?, page, per_page) do
    # 1) Fetch the main embedding, only filtering when m/g are non-nil
    emb_base =
      Embedding
      |> where([e], e.clip_id == ^main_clip_id)
    emb_base =
      if m,
        do: from([e] in emb_base, where: e.model_name == ^m),
        else: emb_base
    emb_base =
      if g,
        do: from([e] in emb_base, where: e.generation_strategy == ^g),
        else: emb_base

    main_embedding =
      emb_base
      |> select([e], e.embedding)
      |> Repo.one!()

    # 2) Build the neighbor query, again guarding m/g
    neigh_base =
      Embedding
      |> where([e], e.clip_id != ^main_clip_id)

    neigh_base =
      if m,
        do: from([e] in neigh_base, where: e.model_name == ^m),
        else: neigh_base
    neigh_base =
      if g,
        do: from([e] in neigh_base, where: e.generation_strategy == ^g),
        else: neigh_base

    direction = if sort_asc?, do: :asc, else: :desc

    neigh_base
    |> join(:inner, [e], c in Clip, on: c.id == e.clip_id and c.ingest_state == "embedded")
    |> select([e, c], %{
         clip: c,
         similarity: fragment("? <=> ?", e.embedding, ^main_embedding)
       })
    |> order_by([e, _c], [{^direction, fragment("? <=> ?", e.embedding, ^main_embedding)}])
    |> offset(^((page - 1) * per_page))
    |> limit(^per_page)
    |> preload([_e, c], clip_artifacts: [])
    |> Repo.all()
  end
end
