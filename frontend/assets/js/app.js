import { Socket } from "phoenix"
import { LiveSocket } from "phoenix_live_view"

import { ReviewHotkeys } from "./review-hotkeys"
import { SpritePlayerController, ThumbHoverPlayer } from "./sprite-player"
import { HoverPlay } from "./hover-play";

// Pull the CSRF token from the page
let csrfToken = document
  .querySelector("meta[name='csrf-token']")
  ?.getAttribute("content")

// Build the hooks object matching your `phx-hook` names in templates
let Hooks = {
  ReviewHotkeys,
  SpritePlayer: SpritePlayerController,
  ThumbHoverPlayer,
  HoverPlay
}

// Initialise LiveSocket with our hooks and CSRF
let liveSocket = new LiveSocket("/live", Socket, {
  hooks: Hooks,
  params: { _csrf_token: csrfToken }
})

// Connect if any LiveViews are on the page
liveSocket.connect()

// Expose for web console debug
window.liveSocket = liveSocket