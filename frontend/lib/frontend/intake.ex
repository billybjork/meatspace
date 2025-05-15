defmodule Frontend.Intake do
  @moduledoc """
  Inserts a row into `source_videos` **and** kicks off the *Intake Source Video*
  deployment on your Prefect server.

  ## Required env-vars

    * `PREFECT_API_URL`          – e.g. http://meatspace:4200/api
    * `INTAKE_DEPLOYMENT_SLUG`   – e.g. "meatspace-ingest/intake-default"
  """

  alias Frontend.Repo

  @source_videos "source_videos"

  @spec submit(String.t()) :: :ok | {:error, String.t()}
  def submit(url) when is_binary(url) do
    with {:ok, id} <- insert_source_video(url),
         :ok       <- create_prefect_run(id, url) do
      :ok
    end
  end

  # ────────────────────────────────────────────────────────────────────
  # step 1 · create DB row
  # ────────────────────────────────────────────────────────────────────
  defp insert_source_video(url) do
    is_http? = String.starts_with?(url, ["http://", "https://"])

    title =
      if is_http? do
        "?"                                     # overwritten later
      else
        url |> Path.basename(".mp4") |> String.slice(0, 250)
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

  # ────────────────────────────────────────────────────────────────────
  # step 2 · call Prefect API
  # ────────────────────────────────────────────────────────────────────
  defp create_prefect_run(id, url) do
    api =
      System.get_env("PREFECT_API_URL") ||
        "http://localhost:4200/api"

    slug =
      System.get_env("INTAKE_DEPLOYMENT_SLUG") ||
        raise """
        Missing INTAKE_DEPLOYMENT_SLUG (e.g. "meatspace-ingest/intake-default")
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

    Req.post(
      url: "#{api}/deployments/name/#{slug}/create_flow_run",
      json: body
    )
    |> handle_response()
  end

  defp handle_response({:ok, %{status: 201}}), do: :ok

  defp handle_response({:ok, %{status: status, body: resp}}),
    do: {:error, "Prefect API #{status}: #{inspect(resp)}"}

  defp handle_response({:error, %Mint.TransportError{reason: reason}}),
    do: {:error, "Prefect connection failed: #{reason}"}

  defp handle_response({:error, other}),
    do: {:error, "Prefect call failed: #{inspect(other)}"}
end
