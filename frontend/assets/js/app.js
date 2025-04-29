// Import dependencies
import { Socket } from "phoenix";
import { LiveSocket } from "phoenix_live_view";
import topbar from "../vendor/topbar";

// Import your custom hooks
import { SpritePlayer } from "./sprite-player";

// Set up hooks
let Hooks = { SpritePlayer };

// Configure LiveSocket
let liveSocket = new LiveSocket("/live", Socket, { hooks: Hooks });

// Show a topbar loading indicator during LiveView navigation and form submits
topbar.config({ barColors: { 0: "#29d" }, shadowColor: "rgba(0, 0, 0, .3)" });
window.addEventListener("phx:page-loading-start", info => topbar.show(300));
window.addEventListener("phx:page-loading-stop", info => topbar.hide());

// Connect if there are any LiveViews on the page
liveSocket.connect();

// Expose liveSocket on window for web console debug logs and latency simulation:
// >> liveSocket.enableDebug()
// >> liveSocket.enableLatencySim(1000)
window.liveSocket = liveSocket;