// static/js/review-main.js

/**
 * review-main.js
 *
 * Handles initialization and cleanup of SpritePlayer instances
 * on the clip review page, especially reacting to HTMX content swaps.
 * Also prepares for keyboard shortcut integration.
 */

(function() {
    'use strict';
    console.log("[Main] review-main.js executing."); // Log script start

    // --- State ---
    let activePlayer = null; // Holds the current active SpritePlayer instance

    // --- Functions ---

    /**
     * Placeholder function to be called by SpritePlayer when the frame updates.
     * Used for integrating with other UI components like split controls.
     * @param {number} clipId - The ID of the clip being played.
     * @param {number} currentFrame - The current frame number displayed.
     * @param {object} meta - The metadata object for the player.
     */
    function updateSplitUI(clipId, currentFrame, meta) {
        // console.log(`[Main] updateSplitUI called: Clip ${clipId}, Frame ${currentFrame}`); // Keep commented unless debugging split UI
        // TODO: Implement logic to update any split-related UI elements here
    }

    /**
     * Cleans up the currently active SpritePlayer instance, if one exists.
     * This is crucial before initializing a new player after an HTMX swap.
     */
    function cleanupActivePlayer() {
        if (activePlayer) {
            console.log(`[Main] Cleaning up player instance for Clip ID: ${activePlayer.clipId}`);
            try {
                activePlayer.cleanup(); // Call the player's cleanup method
            } catch (error) {
                console.error(`[Main] Error during player cleanup for Clip ID ${activePlayer.clipId}:`, error);
            } finally {
                activePlayer = null; // Clear the reference
            }
        } else {
            // console.log("[Main] No active player to clean up."); // Keep commented unless debugging cleanup logic
        }
    }

    /**
     * Initializes a SpritePlayer instance based on the presence of
     * player elements within a given container (document or HTMX target).
     * @param {Element|Document} containerElement - The element to search within for player components.
     */
    function initializePlayer(containerElement) {
        console.log("[Main] initializePlayer called with container:", containerElement);
        if (!containerElement) {
            console.warn("[Main] initializePlayer: null containerElement.");
            return;
        }

        // Find the main player container div using the data attribute
        // Search only within the provided containerElement
        const playerContainer = containerElement.querySelector('.clip-player-instance[data-clip-id]');
        console.log("[Main] initializePlayer: Found player container:", playerContainer);

        if (!playerContainer) {
            console.log("[Main] No player container found in this content scope."); // Modified log
            return; // No player to initialize in this content
        }

        const clipId = playerContainer.dataset.clipId;
        if (!clipId) {
            console.error("[Main] Player container found, but data-clip-id attribute is missing.");
            return;
        }
        console.log(`[Main] Found player container for Clip ID: ${clipId}. Initializing...`);

        // Find the necessary elements using the IDs generated in Python
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

        // Parse metadata
        let meta;
        try {
            meta = JSON.parse(metaScriptElement.textContent);
            console.log(`[Main] initializePlayer: Successfully parsed metadata for Clip ${clipId}. Meta Valid: ${meta?.isValid}, URL: ${meta?.spriteUrl}`); // Log validity and URL
        } catch (e) {
            console.error(`[Main] Failed to parse metadata JSON for Clip ${clipId}:`, e);
            if (viewerElement) {
                viewerElement.textContent = 'Error: Could not load metadata.';
                viewerElement.classList.add('no-sprite');
            }
            if (scrubElement) scrubElement.disabled = true;
            if (playPauseBtn) playPauseBtn.disabled = true;
            return; // Cannot proceed without valid metadata
        }

        // Ensure player is cleaned up *before* creating a new one
        cleanupActivePlayer();

        // Create and store the new SpritePlayer instance
        try {
            console.log(`[Main] initializePlayer: Attempting to create SpritePlayer for Clip ${clipId}...`);
            activePlayer = new SpritePlayer(
                clipId,
                viewerElement,
                scrubElement,
                playPauseBtn,
                frameDisplayElement,
                meta, // Pass the parsed metadata object
                updateSplitUI // Pass the callback function
            );
            console.log(`[Main] SpritePlayer instance CREATED successfully for Clip ID: ${clipId}`);

        } catch (error) {
            console.error(`[Main] Error CREATING SpritePlayer instance for Clip ID ${clipId}:`, error);
            if (viewerElement) {
                 viewerElement.textContent = 'Error: Failed to initialize player.';
                 viewerElement.classList.add('no-sprite');
            }
            activePlayer = null; // Ensure activePlayer is null on failure
        }
    }

    // --- Event Listeners ---

    // 1. Initialize player on initial page load
    document.addEventListener('DOMContentLoaded', () => {
        console.log("[Main] DOMContentLoaded event fired. Initializing player...");
        initializePlayer(document); // Search the whole document
    });

    // 2. Listen for HTMX swaps to re-initialize or clean up
    document.body.addEventListener('htmx:afterSwap', function(event) {
        console.log("[Main] htmx:afterSwap event detected. Target:", event.detail.target.id); // Log target

        const swapTargetId = event.detail.target.id;
        const swappedElement = event.detail.elt; // The new content that was swapped in

        // Determine if the swapped content contains a player
        // Check the swapped element itself OR its children
        const eltContainsPlayer = swappedElement.matches('.clip-player-instance[data-clip-id]') ||
                                  swappedElement.querySelector('.clip-player-instance[data-clip-id]') !== null;


        if (swapTargetId === 'review-container' || (swapTargetId !== 'undo-toast-area' && eltContainsPlayer) ) {
            // If the main container was swapped OR
            // if the target *wasn't* the toast area BUT the new content *does* contain a player
            // (This handles cases where maybe only part of the UI containing the player is swapped)
            console.log(`[Main] Swap appears to contain new player content (Target: ${swapTargetId}, ContainsPlayer: ${eltContainsPlayer}). Re-initializing.`);
            cleanupActivePlayer();
            initializePlayer(swappedElement); // Initialize within the NEWLY swapped content
        } else if (swapTargetId === 'undo-toast-area') {
            // If only the toast area was updated
            console.log("[Main] Swap targeted undo-toast-area. No player re-initialization needed.");
            // Check if the response content is empty (signalling toast removal)
             const responseContent = event.detail.xhr.responseText.trim();
             if (responseContent === '' || responseContent === '<div></div>') {
                 console.log("[Main] Undo toast likely removed.");
             }
        } else {
             console.log(`[Main] htmx:afterSwap event ignored for target: ${swapTargetId}`);
        }
    });

    // 3. Cleanup on page unload
    window.addEventListener('beforeunload', () => {
        console.log("[Main] beforeunload event fired. Cleaning up active player.");
        cleanupActivePlayer();
    });

    // --- Keyboard Shortcut Setup (Placeholder) ---
    console.log("[Main] review-main.js setup complete.");

})(); // End IIFE