defmodule FrontendWeb.VideoController do
  use FrontendWeb, :controller
  alias Frontend.Intake

  def create(conn, %{"url" => url}) do
    case Intake.submit(url) do
      :ok            ->
        conn
        |> put_flash(:info, "Submitted!")
        |> redirect(to: get_referrer(conn))

      {:error, msg} ->
        conn
        |> put_flash(:error, msg)
        |> redirect(to: get_referrer(conn))
    end
  end

  # Extract only the path (and query string, if any) from the Referer header.
  defp get_referrer(conn) do
    conn
    |> get_req_header("referer")
    |> List.first()
    |> case do
      nil ->
        ~p"/"

      ref ->
        %URI{path: path, query: q} = URI.parse(ref)
        if q, do: path <> "?" <> q, else: path
    end
  end
end
