(() => {
  // js/review-main.js
  (function() {
    "use strict";
    console.log("[Main] review-main.js executing.");
    let activePlayer = null;
    function updateSplitUI(clipId, currentFrame, meta) {
    }
    function cleanupActivePlayer() {
      if (activePlayer) {
        console.log(`[Main] Cleaning up player instance for Clip ID: ${activePlayer.clipId}`);
        try {
          activePlayer.cleanup();
        } catch (error) {
          console.error(`[Main] Error during player cleanup for Clip ID ${activePlayer.clipId}:`, error);
        } finally {
          activePlayer = null;
        }
      }
    }
    function initializePlayer(containerElement) {
      console.log("[Main] initializePlayer called with container:", containerElement);
      if (!containerElement) {
        console.warn("[Main] initializePlayer: null containerElement.");
        return;
      }
      const playerContainer = containerElement.querySelector(".clip-player-instance[data-clip-id]");
      console.log("[Main] initializePlayer: Found player container:", playerContainer);
      if (!playerContainer) {
        console.log("[Main] No player container found in this content scope.");
        return;
      }
      const clipId = playerContainer.dataset.clipId;
      if (!clipId) {
        console.error("[Main] Player container found, but data-clip-id attribute is missing.");
        return;
      }
      console.log(`[Main] Found player container for Clip ID: ${clipId}. Initializing...`);
      const viewerElement = playerContainer.querySelector(`#viewer-${clipId}`);
      const scrubElement = playerContainer.querySelector(`#scrub-${clipId}`);
      const playPauseBtn = playerContainer.querySelector(`#playpause-${clipId}`);
      const frameDisplayElement = playerContainer.querySelector(`#frame-display-${clipId}`);
      const metaScriptElement = playerContainer.querySelector(`#meta-${clipId}`);
      console.log(`[Main] initializePlayer: Found viewer:`, viewerElement);
      console.log(`[Main] initializePlayer: Found meta script:`, metaScriptElement);
      if (!viewerElement || !metaScriptElement) {
        console.error(`[Main] Initialization failed for Clip ${clipId}: Missing viewer or meta script element.`);
        return;
      }
      let meta;
      try {
        meta = JSON.parse(metaScriptElement.textContent);
        console.log(`[Main] initializePlayer: Successfully parsed metadata for Clip ${clipId}. Meta Valid: ${meta == null ? void 0 : meta.isValid}, URL: ${meta == null ? void 0 : meta.spriteUrl}`);
      } catch (e) {
        console.error(`[Main] Failed to parse metadata JSON for Clip ${clipId}:`, e);
        if (viewerElement) {
          viewerElement.textContent = "Error: Could not load metadata.";
          viewerElement.classList.add("no-sprite");
        }
        if (scrubElement)
          scrubElement.disabled = true;
        if (playPauseBtn)
          playPauseBtn.disabled = true;
        return;
      }
      cleanupActivePlayer();
      try {
        console.log(`[Main] initializePlayer: Attempting to create SpritePlayer for Clip ${clipId}...`);
        activePlayer = new SpritePlayer(
          clipId,
          viewerElement,
          scrubElement,
          playPauseBtn,
          frameDisplayElement,
          meta,
          updateSplitUI
        );
        console.log(`[Main] SpritePlayer instance CREATED successfully for Clip ID: ${clipId}`);
        if (activePlayer && activePlayer.meta && activePlayer.meta.isValid) {
          console.log(`[Main] Triggering autoplay for Clip ID: ${clipId}`);
          activePlayer.play("autoPlayOnInit");
        } else {
          console.warn(`[Main] Autoplay skipped for Clip ${clipId} due to invalid meta or player creation failure.`);
        }
      } catch (error) {
        console.error(`[Main] Error CREATING SpritePlayer instance for Clip ID ${clipId}:`, error);
        if (viewerElement) {
          viewerElement.textContent = "Error: Failed to initialize player.";
          viewerElement.classList.add("no-sprite");
        }
        activePlayer = null;
      }
    }
    document.addEventListener("DOMContentLoaded", () => {
      console.log("[Main] DOMContentLoaded event fired. Initializing player...");
      initializePlayer(document);
    });
    document.body.addEventListener("htmx:afterSwap", function(event) {
      console.log("[Main] htmx:afterSwap event detected. Target:", event.detail.target.id);
      const swapTargetId = event.detail.target.id;
      const swappedElement = event.detail.elt;
      if (swapTargetId === "review-container") {
        console.log("[Main] Swap targeted review-container. Re-initializing player within new content.");
        cleanupActivePlayer();
        initializePlayer(swappedElement);
      } else {
        console.log(`[Main] htmx:afterSwap event ignored for target: ${swapTargetId}`);
      }
    });
    window.addEventListener("beforeunload", () => {
      console.log("[Main] beforeunload event fired. Cleaning up active player.");
      cleanupActivePlayer();
    });
    console.log("[Main] review-main.js setup complete.");
  })();
})();
