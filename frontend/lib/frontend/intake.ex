defmodule Frontend.Intake do
  @moduledoc """
  Pure-Elixir helper that

    1. Inserts a **new** row into `source_videos`.
    2. Triggers your Prefect deployment (`intake_task`).

  Expects:

    * `PREFECT_API_URL`         – defaults to http://localhost:4200/api
    * `INTAKE_DEPLOYMENT_ID`    – deployment id/slug for *intake_task*
  """

  alias Frontend.Repo

  # now used in insert_source_video/1
  @source_videos "source_videos"

  @spec submit(String.t()) :: :ok | {:error, String.t()}
  def submit(url) when is_binary(url) do
    with {:ok, id} <- insert_source_video(url),
         :ok       <- create_prefect_run(id, url) do
      :ok
    end
  end

    # -- step 1: insert into source_videos -------------------------------

    defp insert_source_video(url) do
    is_http? = String.starts_with?(url, ["http://", "https://"])

    # For URLs give a tiny placeholder that satisfies NOT-NULL
    # For local files keep the derived filename
    title =
      if is_http? do
        "?"                  # will be overwritten by intake_task
      else
        url
        |> Path.basename(".mp4")
        |> String.slice(0, 250)
      end

    fields = %{
      title:        title,
      ingest_state: "new",
      original_url: if(is_http?, do: url, else: nil),
      web_scraped:  is_http?,
      created_at:   DateTime.utc_now(),
      updated_at:   DateTime.utc_now()
    }

    case Repo.insert_all(@source_videos, [fields], returning: [:id]) do
      {1, [%{id: id} | _]} -> {:ok, id}
      _                    -> {:error, "DB insert failed"}
    end
  rescue
    e -> {:error, "DB error: #{Exception.message(e)}"}
  end

  # -- step 2: call Prefect API ----------------------------------------

  defp create_prefect_run(id, url) do
    api        = System.get_env("PREFECT_API_URL") || "http://localhost:4200/api"
    deployment =
      System.get_env("INTAKE_DEPLOYMENT_ID") ||
        raise """
        Missing INTAKE_DEPLOYMENT_ID. \
        Please export the ID of your Intake Source Video deployment.
        """

    body = %{
      parameters: %{
        "source_video_id"    => id,
        "input_source"       => url,
        "re_encode_for_qt"   => true,
        "overwrite_existing" => false
      },
      idempotency_key: "frontend_submit_#{id}"
    }

    case Req.post(
           url: "#{api}/deployments/#{deployment}/create_flow_run",
           json: body
         ) do
      {:ok, %{status: 201}} ->
        :ok

      {:ok, %{status: status, body: resp}} ->
        {:error, "Prefect API #{status}: #{inspect(resp)}"}

      {:error, %Mint.TransportError{reason: reason}} ->
        {:error, "Prefect connection failed: #{reason}"}

      {:error, other} ->
        {:error, "Prefect call failed: #{inspect(other)}"}
    end
  end
end
