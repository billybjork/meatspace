document.addEventListener('DOMContentLoaded', () => {
    const reviewQueue = document.getElementById('review-queue');
    window.spritePlayers = {}; // Store player instances globally, keyed by clipId
    const players = window.spritePlayers;
    const splitModeState = {}; // Store split mode status, keyed by clipId

    // --- Global State for Undo ---
    let lastUndoableClipId = null; // Track the clip ID of the last action that can be undone

    /**
     * Updates the globally tracked clip ID for the undo action (Ctrl+Z).
     * @param {string|null} clipId - The clip ID to track, or null to clear.
     */
    const setLastUndoableClipId = (clipId) => {
        console.log("Setting last undoable Clip ID:", clipId);
        lastUndoableClipId = clipId;
    };
    // --------------------------------

    // --- Initialization Loop ---
    // Initialize sprite players and UI elements for each clip item
    document.querySelectorAll('.clip-review-item').forEach(clipItem => {
        const clipId = clipItem.dataset.clipId;
        const spriteUrl = clipItem.dataset.spriteUrl;
        const spriteMetaJson = clipItem.dataset.spriteMeta; // Expects escaped JSON string
        const viewer = document.getElementById(`sprite-viewer-${clipId}`);
        const scrubBar = document.getElementById(`scrub-${clipId}`);
        const playPauseBtn = clipItem.querySelector('.sprite-play-pause-btn');
        const frameDisplay = document.getElementById(`frame-display-${clipId}`);
        const controlsDiv = document.getElementById(`controls-${clipId}`);
        const splitBtn = clipItem.querySelector('.split-mode-btn');
        const retryBtn = clipItem.querySelector('.retry-sprite-btn'); // Get retry button if present

        splitModeState[clipId] = { isActive: false }; // Initialize split state for this clip
        let meta = null;
        let initError = null;

        if (!viewer) {
            console.error(`[Main Init ${clipId}] Critical error: Sprite viewer element not found.`);
            return; // Skip this item if viewer is missing
        }
        viewer.textContent = 'Initializing...'; // Initial message

        // Attempt to parse metadata and initialize player if data is present
        if (spriteUrl && spriteMetaJson) {
            try {
                // Metadata might be double-encoded (JSON string inside JSON) due to templating/escaping
                let parsedOnce = JSON.parse(spriteMetaJson);
                meta = (typeof parsedOnce === 'string') ? JSON.parse(parsedOnce) : parsedOnce;

                if (!meta || typeof meta !== 'object') {
                    throw new Error("Parsed metadata is not a valid object.");
                }

                // Basic validation and type conversion for essential metadata fields
                const parsedMeta = {
                    tile_width: parseFloat(meta.tile_width),
                    tile_height_calculated: parseFloat(meta.tile_height_calculated),
                    cols: parseInt(meta.cols, 10),
                    rows: parseInt(meta.rows, 10),
                    total_sprite_frames: parseInt(meta.total_sprite_frames, 10),
                    clip_fps: parseFloat(meta.clip_fps),
                    clip_total_frames: parseInt(meta.clip_total_frames, 10),
                    spriteUrl: spriteUrl // Include URL for the player
                };

                // Validate that essential numeric properties are valid numbers > 0
                const requiredKeys = ['tile_width', 'tile_height_calculated', 'cols', 'rows', 'total_sprite_frames', 'clip_fps', 'clip_total_frames'];
                for (const key of requiredKeys) {
                    if (isNaN(parsedMeta[key]) || parsedMeta[key] <= 0) {
                        throw new Error(`Invalid or missing numeric value for '${key}': ${meta[key]}`);
                    }
                }
                parsedMeta.isValid = true; // Mark metadata as valid

                // Create and store the SpritePlayer instance
                players[clipId] = new SpritePlayer(clipId, viewer, scrubBar, playPauseBtn, frameDisplay, parsedMeta, updateSplitUI); // Pass split UI updater
                console.log(`[Main Init ${clipId}] SpritePlayer initialized successfully.`);

            } catch (e) {
                console.error(`[Main Init ${clipId}] Failed during player initialization:`, e);
                initError = `Error initializing sprite: ${e.message}`;
                viewer.textContent = initError;
                viewer.classList.add('no-sprite');
            }
        } else {
            // Handle cases where sprite URL or metadata is missing from the start
            const spriteErrorMsg = clipItem.querySelector('.clip-info strong[style*="color:orange"]'); // Check for backend failure message
            initError = spriteErrorMsg ? 'Sprite generation failed (backend).' : 'Sprite sheet unavailable.';
            viewer.textContent = initError;
            viewer.classList.add('no-sprite');
        }

        // Disable controls if initialization failed or sprite is unavailable
        if (initError) {
            console.warn(`[Main Init ${clipId}] Disabling controls due to: ${initError}`);
            if (scrubBar) scrubBar.disabled = true;
            if (playPauseBtn) playPauseBtn.disabled = true;
            if (controlsDiv) controlsDiv.style.opacity = '0.5';
            if (splitBtn) {
                splitBtn.disabled = true;
                splitBtn.title = "Cannot split: Sprite unavailable or failed.";
            }
            // Ensure retry button is enabled *only* if a sprite error occurred
            if (retryBtn && initError.includes('failed')) {
                retryBtn.disabled = false;
            } else if (retryBtn) {
                retryBtn.disabled = true; // Disable retry if sprite is just missing, not failed
            }
        }

        // Add focus/blur listeners and keydown listener directly to the viewer
        // This helps manage focus state and captures keys when the viewer is active
         viewer.setAttribute('tabindex', '0'); // Make it focusable
         viewer.addEventListener('focus', () => {
              const player = players[clipId];
              if (player) player.isFocusedForKeys = true;
              viewer.style.outline = '2px solid dodgerblue'; // Visual focus indicator
         });
         viewer.addEventListener('blur', () => {
              const player = players[clipId];
              if (player) player.isFocusedForKeys = false;
              viewer.style.outline = 'none';
         });
         // Attach the keydown handler directly to the viewer element
         viewer.addEventListener('keydown', (event) => handleViewerKeydown(event, clipItem));

    }); // --- End of Initialization Loop ---


    // --- Split Mode UI Management Functions ---

    /**
     * Activates the split mode UI for a specific clip.
     * @param {string} clipId - The ID of the clip.
     * @param {HTMLElement} clipItem - The DOM element for the clip item.
     * @param {SpritePlayer} player - The SpritePlayer instance for the clip.
     */
    function activateSplitMode(clipId, clipItem, player) {
        if (!player || !player.meta?.isValid) {
            alert("Cannot enter split mode: Sprite data invalid or player not initialized.");
            return;
        }
        splitModeState[clipId].isActive = true;
        player.pause(); // Ensure playback is paused

        const splitControls = clipItem.querySelector('.split-controls');
        const splitModeBtn = clipItem.querySelector('.split-mode-btn');
        const splitFeedback = clipItem.querySelector('.split-feedback');
        const viewer = document.getElementById(`sprite-viewer-${clipId}`);
        const allActionButtons = clipItem.querySelectorAll('.clip-actions > .action-btn:not(.split-mode-btn)'); // Select non-split action buttons

        if (splitControls) splitControls.style.display = 'block';
        if (splitModeBtn) splitModeBtn.style.display = 'none'; // Hide the "Enter Split Mode" button
        if (splitFeedback) splitFeedback.textContent = ''; // Clear previous feedback

        // Disable other action buttons while in split mode
        allActionButtons.forEach(btn => btn.disabled = true);

        // Update the split UI immediately with the current frame
        const currentFrame = player.getCurrentFrame();
        updateSplitUI(clipId, currentFrame, player.meta); // Update labels and button state

        if (viewer) viewer.focus(); // Set focus to the viewer for keyboard controls (arrows, Enter, Esc)
        console.log(`[Main ${clipId}] Split mode activated.`);
    }

    /**
     * Deactivates the split mode UI for a specific clip.
     * @param {string} clipId - The ID of the clip.
     * @param {HTMLElement} clipItem - The DOM element for the clip item.
     */
    function cancelSplitMode(clipId, clipItem) {
        splitModeState[clipId].isActive = false;

        const splitControls = clipItem.querySelector('.split-controls');
        const splitModeBtn = clipItem.querySelector('.split-mode-btn');
        const viewer = document.getElementById(`sprite-viewer-${clipId}`);
        const allActionButtons = clipItem.querySelectorAll('.clip-actions > .action-btn:not(.split-mode-btn)'); // Select non-split action buttons

        if (splitControls) splitControls.style.display = 'none';
        if (splitModeBtn) splitModeBtn.style.display = 'inline-block'; // Show the "Enter Split Mode" button again

        // Re-enable other action buttons (if they weren't disabled for other reasons initially)
        // Check original disabled state if necessary, but simple re-enable is usually fine here.
        allActionButtons.forEach(btn => btn.disabled = false);
        // Special check for merge button based on its data attribute
        const mergeBtn = clipItem.querySelector('.merge-next-btn');
        if (mergeBtn && clipItem.dataset.canMergeNext === 'false') { // Check original data attribute
             mergeBtn.disabled = true;
        }


        if (viewer) viewer.focus(); // Optional: refocus viewer after cancellation
        console.log(`[Main ${clipId}] Split mode cancelled.`);
    }

    /**
     * Updates the UI elements within the split controls panel based on the selected frame.
     * Called by the SpritePlayer on frame updates or when split mode is activated.
     * @param {string} clipId - The ID of the clip being updated.
     * @param {number} frameNumber - The currently selected frame number (0-based).
     * @param {object} meta - The validated sprite metadata object.
     */
    function updateSplitUI(clipId, frameNumber, meta) {
        // Only update if split mode is active for this clip
        if (!splitModeState[clipId]?.isActive) return;

        const clipItem = document.getElementById(`clip-${clipId}`);
        if (!clipItem || !meta || !meta.isValid) return; // Need item and valid metadata

        // Get references to UI elements within the split controls
        const confirmFrameSpan = clipItem.querySelector('.split-confirm-frame');
        const confirmTimeSpan = clipItem.querySelector('.split-confirm-time');
        const confirmFrameBtnText = clipItem.querySelector('.split-confirm-frame-btn-text');
        const confirmSplitBtn = clipItem.querySelector('.confirm-split-btn');
        const splitFeedback = clipItem.querySelector('.split-feedback');
        const totalFramesSpan = clipItem.querySelector('.split-total-frames');

        // Ensure all elements exist before proceeding
        if (!confirmFrameSpan || !confirmTimeSpan || !confirmFrameBtnText || !confirmSplitBtn || !splitFeedback || !totalFramesSpan) {
             console.warn(`[Split UI ${clipId}] Missing one or more split control elements.`);
             return;
        }

        // Calculate display values
        const totalFrames = meta.clip_total_frames; // Total frames in the original clip segment
        const fps = meta.clip_fps;
        const displayTotalFrames = totalFrames > 0 ? totalFrames - 1 : '?'; // Display is 0 to N-1
        const approxTime = (fps > 0 && totalFrames > 0) ? (frameNumber / fps) : 0; // Approx time based on frame

        // Update displayed text
        confirmFrameSpan.textContent = frameNumber;
        confirmFrameBtnText.textContent = frameNumber;
        totalFramesSpan.textContent = displayTotalFrames;
        confirmTimeSpan.textContent = approxTime.toFixed(3); // Format time to 3 decimal places

        // --- Split Frame Validation Logic ---
        const minFrameMargin = 1; // Cannot split at the very first (0) or very last frame (N-1)
        let isDisabled = true;
        let feedbackMsg = '';
        let isError = true;

        // Check if the clip is long enough to be split at all
        if (totalFrames <= (2 * minFrameMargin)) {
            feedbackMsg = `Clip too short to split (Total Frames: ${totalFrames})`;
        }
        // Check if selected frame is within the valid range (must be >= margin and < totalFrames - margin)
        else if (frameNumber < minFrameMargin) {
            feedbackMsg = `Select frame ≥ ${minFrameMargin}.`;
        } else if (frameNumber >= (totalFrames - minFrameMargin)) {
            // Frame numbers are 0 to N-1. Max allowed split frame is N-2.
            feedbackMsg = `Select frame ≤ ${totalFrames - minFrameMargin - 1}.`;
        } else {
            // Frame is valid
            isDisabled = false;
            feedbackMsg = 'Valid split point.';
            isError = false;
        }

        // Update button state and feedback message/style
        confirmSplitBtn.disabled = isDisabled;
        splitFeedback.textContent = feedbackMsg;
        splitFeedback.style.color = isError ? '#d00' : '#080'; // Red for error/invalid, Green for valid

    } // --- End of updateSplitUI ---


    // --- Main Click Event Listener (Delegated from reviewQueue) ---
    reviewQueue.addEventListener('click', async (event) => {
        const target = event.target;
        const clipItem = target.closest('.clip-review-item'); // Find the parent clip item

        // Ignore clicks outside of clip items or on elements not meant to be interactive here
        if (!clipItem) return;

        // Ignore clicks if the item is currently processing
        if (clipItem.classList.contains('processing')) {
            console.log("Ignoring click: Item is processing.");
            return;
        }

        const clipId = clipItem.dataset.clipId;
        const player = players[clipId]; // Get the player instance (might be undefined if init failed)
        const feedbackDiv = clipItem.querySelector('.action-feedback');
        const undoButton = clipItem.querySelector('.undo-button');
        const splitControls = clipItem.querySelector('.split-controls');

        // --- Player Controls ---
        if (target.matches('.sprite-play-pause-btn')) {
            if (player) player.togglePlayback();
            return; // Handled
        }

        // --- Split Mode Actions ---
        if (target.matches('.split-mode-btn') && !target.disabled) {
            activateSplitMode(clipId, clipItem, player);
            return; // Handled
        }
        if (target.matches('.cancel-split-btn')) {
            cancelSplitMode(clipId, clipItem);
            return; // Handled
        }
        if (target.matches('.confirm-split-btn') && !target.disabled) {
            if (player) {
                player.pause(); // Ensure paused before submitting
                const frameToSplit = player.getCurrentFrame();
                // Call the action handler, passing necessary elements and the state setter
                const result = await queueSplit(clipId, frameToSplit, clipItem, feedbackDiv, splitControls, undoButton, setLastUndoableClipId);
                if (result.success) {
                    // Clean up player instance if split was successfully queued
                    if (players[clipId]) {
                        players[clipId].cleanup();
                        delete players[clipId];
                    }
                    delete splitModeState[clipId]; // Remove split state entry
                } else {
                    // Optionally re-enable split mode controls on failure?
                     cancelSplitMode(clipId, clipItem); // Revert UI back from split mode on failure
                }
            } else {
                console.error(`[Confirm Split ${clipId}] Cannot confirm split: Player instance not found.`);
                const splitFeedback = splitControls?.querySelector('.split-feedback');
                if (splitFeedback) splitFeedback.textContent = "Error: Player not initialized.";
            }
            return; // Handled
        }

        // --- Standard Actions (Approve, Skip, Archive, Merge, Retry) & Undo ---
        if (target.matches('.action-btn') && !target.disabled && !target.matches('.split-mode-btn') && !target.matches('.confirm-split-btn') && !target.matches('.cancel-split-btn')) {
            const action = target.dataset.action;
            if (!action) return; // Ignore if button has no action defined

            if (player) player.pause(); // Pause playback before taking action

            // Call the action handler, passing necessary elements and the state setter
            const result = await handleAction(clipId, action, clipItem, feedbackDiv, undoButton, setLastUndoableClipId);
            if (result.success) {
                // Clean up player instance if action implies completion (approve, archive, merge)
                const isTerminalAction = ['approve', 'archive', 'merge_next'].includes(action);
                 if (isTerminalAction && players[clipId]) {
                    players[clipId].cleanup();
                    delete players[clipId];
                    delete splitModeState[clipId];
                 }
                 // If retry succeeded, we might need to re-initialize the player (requires page refresh currently)
                 if (action === 'retry_sprite_gen') {
                      showFeedback(feedbackDiv, result.message || "Success: Retrying sprite generation. Refresh to see results.", false);
                      // Technically could try re-init here, but refresh is simpler/safer
                 }
            }
            return; // Handled
        }
        if (target.matches('.undo-button') && !target.disabled) {
             if (player) player.pause();

             // Call the undo handler, passing necessary elements and the state setter
             const result = await handleUndo(clipId, clipItem, feedbackDiv, undoButton, setLastUndoableClipId);
             if (result.success) {
                 // Clean up player instance on successful undo, as state is reverted.
                 // Re-initialization will happen on page refresh.
                  if(players[clipId]) {
                     players[clipId].cleanup();
                     delete players[clipId];
                  }
                  delete splitModeState[clipId];
                  // Feedback already suggests refreshing.
             }
            return; // Handled
        }

    }); // --- End of Main Click Event Listener ---


    // --- Keyboard Event Handling Function ---
    /**
     * Handles keydown events, primarily when a sprite viewer is focused,
     * but also handles global Ctrl+Z.
     * @param {KeyboardEvent} event - The keydown event object.
     * @param {HTMLElement | null} clipItem - The clip item element associated with the event (if any).
     */
    function handleViewerKeydown(event, clipItem) {
        const clipId = clipItem?.dataset.clipId; // Use optional chaining
        const player = clipId ? players[clipId] : null;
        const state = clipId ? splitModeState[clipId] : null;

        // --- Ctrl+Z / Cmd+Z for Undo (Global or Focused) ---
        // Handles the undo shortcut regardless of exact focus, uses tracked ID.
        if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'z') {
            event.preventDefault(); // Prevent browser's default undo
            console.log("Undo shortcut (Ctrl+Z) detected. Last undoable clip ID:", lastUndoableClipId);

            if (lastUndoableClipId) {
                const targetClipItem = document.getElementById(`clip-${lastUndoableClipId}`);
                if (targetClipItem && !targetClipItem.classList.contains('processing')) { // Check if item exists and isn't busy
                    const targetUndoButton = targetClipItem.querySelector('.undo-button');
                    // Check if the button exists AND is currently visible (style.display is not 'none')
                    if (targetUndoButton && window.getComputedStyle(targetUndoButton).display !== 'none') {
                        console.log(`Triggering undo via shortcut for clip ${lastUndoableClipId}`);
                        targetUndoButton.click(); // Programmatically click the visible undo button
                    } else {
                         console.log(`Undo button for clip ${lastUndoableClipId} not found or not visible.`);
                         // Optionally provide user feedback (e.g., subtle flash)
                    }
                } else {
                    console.log(`Clip item for ID ${lastUndoableClipId} not found or is processing.`);
                }
            } else {
                console.log("No action to undo.");
                // Optional: User feedback indicating no undo available
            }
            return; // Stop further processing for Ctrl+Z
        }


        // --- Viewer-Specific Keys (Require Focus and Valid Player) ---
        // These actions only make sense if focus is within a specific clip item
        // and the corresponding player is valid.
        if (!clipItem || !player || !player.meta?.isValid) {
            // If Ctrl+Z wasn't handled above, and there's no valid player context, do nothing more.
            return;
        }

        // Spacebar: Toggle Play/Pause (only if viewer is focused)
        if (event.code === 'Space' && document.activeElement === player.viewerElement) {
            event.preventDefault(); // Prevent page scroll
            player.togglePlayback();
            return;
        }

        // Arrow Keys: Scrub Frames (only if viewer is focused)
        let frameChange = 0;
        if (event.key === 'ArrowLeft') frameChange = -1;
        else if (event.key === 'ArrowRight') frameChange = 1;

        if (frameChange !== 0 && document.activeElement === player.viewerElement) {
            event.preventDefault();
            player.pause(); // Pause on manual scrub
            const currentFrame = player.getCurrentFrame();
            const newFrame = currentFrame + frameChange;
            player.updateFrame(newFrame, true); // Force UI update for player and scrub bar

            // If in split mode, the player's updateFrame should trigger updateSplitUI via the callback
            // No need to call updateSplitUI directly here if callback is set up.
            return;
        }

        // Enter/Escape: Only handle when in Split Mode for this clip
        if (state?.isActive && document.activeElement === player.viewerElement) {
             const confirmSplitBtn = clipItem.querySelector('.confirm-split-btn');
             const cancelSplitBtn = clipItem.querySelector('.cancel-split-btn');

             if (event.key === 'Enter' && confirmSplitBtn && !confirmSplitBtn.disabled) {
                 event.preventDefault();
                 console.log("[Keyboard] Enter pressed in split mode - clicking confirm");
                 confirmSplitBtn.click(); // Simulate click on the confirm button
             } else if (event.key === 'Escape' && cancelSplitBtn) {
                  event.preventDefault();
                  console.log("[Keyboard] Escape pressed in split mode - clicking cancel");
                  cancelSplitBtn.click(); // Simulate click on the cancel button
             }
             // No return here, Enter/Escape might have other default behaviors if buttons are disabled/missing
        }
    } // --- End of handleViewerKeydown ---


    // --- Global Keyboard Listener ---
    // Catches keys pressed anywhere on the page. Useful for global shortcuts like Ctrl+Z.
    document.addEventListener('keydown', (event) => {
        const focusedElement = document.activeElement;
        let associatedClipItem = null;

        // Determine if focus is within a review item context
        if (focusedElement) {
            associatedClipItem = focusedElement.closest('.clip-review-item');
        }

        // Delegate all key handling (including global Ctrl+Z) to the main function
        handleViewerKeydown(event, associatedClipItem);

    }); // --- End of Global Keydown Listener ---


}); // --- End of DOMContentLoaded ---