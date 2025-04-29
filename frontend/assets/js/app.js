import {Socket} from "phoenix"
import {LiveSocket} from "phoenix_live_view"

import {SpritePlayerController} from "./sprite-player"

let csrfToken = document.querySelector("meta[name='csrf-token']")?.getAttribute("content");

let liveSocket = new LiveSocket("/live", Socket, {
  params: {_csrf_token: csrfToken},
  hooks: {
    SpritePlayer: SpritePlayerController
  }
})

// connect if LiveView DOM is present
liveSocket.connect()

// expose for debugging
window.liveSocket = liveSocket