import { Socket } from "phoenix"
import { LiveSocket } from "phoenix_live_view"

// Import your keyboard/controller hooks
import { ReviewHotkeys } from "./review-hotkeys"

// Import both sprite-player hooks from the same file
import { SpritePlayerController, ThumbHoverPlayer } from "./sprite-player"

// Pull the CSRF token from the page
let csrfToken = document
  .querySelector("meta[name='csrf-token']")
  ?.getAttribute("content")

// Build the hooks object matching your `phx-hook` names in templates
let Hooks = {
  ReviewHotkeys,                           // hotkeys (F/G, Shift+Space, etc.)
  SpritePlayer: SpritePlayerController,    // main sprite player hook
  ThumbHoverPlayer                         // hover-autoplay thumbnails
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