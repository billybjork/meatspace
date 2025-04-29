defmodule Frontend.Clips.Schema do
  @moduledoc false
  # Re-usable imports/aliases for every Clips schema
  defmacro __using__(_) do
    quote do
      use Ecto.Schema
      import Ecto.Changeset
      @primary_key {:id, :id, autogenerate: true}
      @timestamps_opts [type: :utc_datetime_usec]
    end
  end
end
