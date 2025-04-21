/**
 * Main JavaScript for the single-clip review UI.
 * Handles player initialization, keyboard shortcuts, split mode management,
 * and delegates API calls to review-actions.js.
 */
document.addEventListener('DOMContentLoaded', () => {
    // --- Global Setup ---
    const reviewContainer = document.querySelector('.review-container');
    const clipItem = reviewContainer?.querySelector('.clip-review-item'); // Expect only one

    // Store references globally for the single player instance
    window.spritePlayer = null; // Use window scope or a local module pattern if preferred
    let currentClipId = null;
    let splitModeActive = false; // Track if split controls are currently shown for the clip

    // Keyboard state tracking
    let keyState = {
        'a': false, 's': false, 'd': false, 'f': false, // Action keys
        'meta': false,    // Cmd on Mac
        'control': false, // Ctrl on Win/Linux
        // We don't need to track Enter/Space/Arrows/Escape state, just their event triggers
    };

    // --- Initialization ---
    if (clipItem) {
        currentClipId = clipItem.dataset.clipId;
        const spriteUrl = clipItem.dataset.spriteUrl;
        const spriteMetaJson = clipItem.dataset.spriteMeta;
        const shouldAutoplay = clipItem.dataset.autoplay === 'true';

        // Get references to the single clip's elements
        const viewer = document.getElementById(`sprite-viewer-${currentClipId}`);
        const scrubBar = document.getElementById(`scrub-${currentClipId}`);
        const playPauseBtn = clipItem.querySelector('.sprite-play-pause-btn');
        const frameDisplay = document.getElementById(`frame-display-${currentClipId}`);
        const splitBtn = clipItem.querySelector('.split-mode-btn'); // For checking enablement
        const retryBtn = clipItem.querySelector('.retry-sprite-btn');

        let meta = null;
        let initError = null;

        if (viewer) {
             viewer.textContent = 'Initializing sprite...'; // Initial message

             // Attempt to parse metadata and initialize player
             if (spriteUrl && spriteMetaJson) {
                 try {
                     // Metadata might be double-encoded
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
                         total_sprite_frames: parseInt(meta.total_sprite_frames, 10), // Frames in the sprite sheet itself
                         clip_fps: parseFloat(meta.clip_fps),
                         clip_total_frames: parseInt(meta.clip_total_frames, 10), // Actual frames in the video clip segment
                         spriteUrl: spriteUrl // Include URL for the player
                     };

                     // Validate that essential numeric properties are valid numbers > 0
                     const requiredKeys = ['tile_width', 'tile_height_calculated', 'cols', 'rows', 'total_sprite_frames', 'clip_fps', 'clip_total_frames'];
                     let missingOrInvalidKey = null;
                     for (const key of requiredKeys) {
                         if (isNaN(parsedMeta[key]) || parsedMeta[key] <= 0) {
                             missingOrInvalidKey = key;
                             break;
                         }
                     }
                     if(missingOrInvalidKey) {
                        throw new Error(`Invalid or missing numeric value for metadata key '${missingOrInvalidKey}': ${meta[missingOrInvalidKey]}`);
                     }
                     parsedMeta.isValid = true; // Mark metadata as valid

                     // --- Create and store the SpritePlayer instance ---
                     // The last argument is the callback function to update split UI on frame changes
                     window.spritePlayer = new SpritePlayer(currentClipId, viewer, scrubBar, playPauseBtn, frameDisplay, parsedMeta, updateSplitUI);
                     console.log(`[Main Init ${currentClipId}] SpritePlayer initialized successfully.`);

                     // Autoplay if requested
                     if (shouldAutoplay && window.spritePlayer) {
                         console.log(`[Main Init ${currentClipId}] Autoplaying sprite...`);
                         // Use a small delay to ensure the browser has rendered and is ready
                         setTimeout(() => window.spritePlayer?.play('autoplay'), 100);
                     }

                     // Focus the viewer for immediate keyboard control after a short delay
                     setTimeout(() => viewer.focus(), 150);

                 } catch (e) {
                     console.error(`[Main Init ${currentClipId}] Failed during player initialization:`, e);
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
                 console.warn(`[Main Init ${currentClipId}] Disabling controls due to: ${initError}`);
                 if (scrubBar) scrubBar.disabled = true;
                 if (playPauseBtn) playPauseBtn.disabled = true;
                 // Find the parent controls div to potentially style it
                 const controlsDiv = document.getElementById(`controls-${currentClipId}`);
                 if (controlsDiv) controlsDiv.style.opacity = '0.6'; // Dim controls

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
             } else {
                 // Ensure retry button is disabled if there was no error
                 if (retryBtn) retryBtn.disabled = true;
             }

        } else {
            console.error("Sprite viewer element (#sprite-viewer-...) not found in the DOM!");
            // Display an error message in the main container if possible
             const feedbackDiv = clipItem?.querySelector('.action-feedback');
             if(feedbackDiv) showFeedback(feedbackDiv, "UI Error: Sprite viewer missing.", true);
        }

    } else {
        console.log("No clip review item found on the page.");
        // The HTML template should show the "No clips" message in this case.
    }


    // --- Split Mode UI Management Functions ---

    /**
     * Activates the split mode UI for the current clip.
     */
    function activateSplitMode() {
        const player = window.spritePlayer;
        // Prevent activating if already active, player is invalid, or sprite failed
        if (splitModeActive || !player || !player.meta?.isValid) {
            console.warn("Cannot activate split mode:", {splitModeActive, playerValid: !!player?.meta?.isValid});
            const feedbackDiv = clipItem?.querySelector('.action-feedback');
            if(feedbackDiv) showFeedback(feedbackDiv, "Cannot enter split mode (sprite unavailable or invalid).", true);
             setTimeout(() => { if(feedbackDiv) feedbackDiv.textContent = ''; }, 3000);
            return;
        }

        player.pause('splitModeActivate'); // Ensure playback is paused
        splitModeActive = true;

        // Get references to elements within the current clip item
        const splitControls = clipItem.querySelector('.split-controls');
        const splitModeBtn = clipItem.querySelector('.split-mode-btn');
        const splitFeedback = clipItem.querySelector('.split-feedback');
        const allActionButtons = clipItem.querySelectorAll('.clip-actions > .action-btn:not(.split-mode-btn)'); // Select non-split action buttons

        if (splitControls) splitControls.style.display = 'block';
        if (splitModeBtn) splitModeBtn.style.display = 'none'; // Hide the "Enter Split Mode" button
        if (splitFeedback) splitFeedback.textContent = ''; // Clear previous feedback

        // Disable other primary action buttons while in split mode
        allActionButtons.forEach(btn => {
             // Keep retry button enabled if it was initially enabled
             if (!btn.classList.contains('retry-sprite-btn') || btn.disabled) {
                btn.disabled = true;
             }
        });

        // Update the split UI immediately with the current frame
        const currentFrame = player.getCurrentFrame();
        updateSplitUI(currentClipId, currentFrame, player.meta); // Update labels and button state

        player.viewerElement?.focus(); // Set focus to the viewer for keyboard controls
        console.log(`[Main ${currentClipId}] Split mode activated.`);
    }

    /**
     * Deactivates the split mode UI for the current clip.
     */
    function cancelSplitMode() {
        if (!splitModeActive) return;
        splitModeActive = false;

        // Get references to elements
        const splitControls = clipItem.querySelector('.split-controls');
        const splitModeBtn = clipItem.querySelector('.split-mode-btn');
        const allActionButtons = clipItem.querySelectorAll('.clip-actions > .action-btn:not(.split-mode-btn)');

        if (splitControls) splitControls.style.display = 'none';
        if (splitModeBtn) {
            // Re-enable the split button ONLY if the player/sprite is valid
            splitModeBtn.style.display = 'inline-block';
            splitModeBtn.disabled = !(window.spritePlayer?.meta?.isValid);
        }

        // Re-enable other action buttons, respecting original disabled states where possible
        allActionButtons.forEach(btn => {
            // A simple approach: re-enable all except merge if it shouldn't be enabled.
            // More complex logic might be needed if buttons have other disabling conditions.
             const mergeBtn = clipItem.querySelector('.merge-previous-btn'); // Find the merge button specifically
             if(btn === mergeBtn) {
                // Re-enable merge ONLY if the template data allows it (check original disabled attribute)
                 const templateDisabled = mergeBtn.getAttribute('disabled') !== null; // Check if disabled was set in HTML
                 btn.disabled = templateDisabled;
             } else if (btn.classList.contains('retry-sprite-btn')) {
                 // Retry enabled only if sprite failed initially
                 const spriteErrorMsg = clipItem.querySelector('.clip-info strong[style*="color:orange"]');
                 btn.disabled = !spriteErrorMsg;
             }
             else {
                 btn.disabled = false; // Re-enable other buttons
             }
        });

        window.spritePlayer?.viewerElement?.focus(); // Optional: refocus viewer
        console.log(`[Main ${currentClipId}] Split mode cancelled.`);
    }

    /**
     * Updates the UI elements within the split controls panel based on the selected frame.
     * Called by the SpritePlayer on frame updates OR when split mode is activated.
     * @param {string} clipId - The ID of the clip being updated (should match currentClipId).
     * @param {number} frameNumber - The currently selected frame number (0-based).
     * @param {object} meta - The validated sprite metadata object.
     */
    function updateSplitUI(clipId, frameNumber, meta) {
        // Only update if split mode is active for the current clip
        if (!splitModeActive || clipId !== currentClipId) return;

        if (!clipItem || !meta || !meta.isValid) {
            console.warn(`[Split UI ${clipId}] Cannot update: Missing clip item or invalid meta.`);
            return;
        }

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
        const displayTotalFrames = totalFrames > 0 ? totalFrames - 1 : 0; // Display is 0 to N-1
        const approxTime = (fps > 0 && totalFrames > 0 && frameNumber >=0) ? (frameNumber / fps) : 0; // Approx time

        // Update displayed text
        confirmFrameSpan.textContent = frameNumber;
        confirmFrameBtnText.textContent = frameNumber; // Update button text too
        totalFramesSpan.textContent = displayTotalFrames; // Show max frame index
        confirmTimeSpan.textContent = approxTime.toFixed(3); // Format time

        // --- Split Frame Validation Logic ---
        const minFrameMargin = 1; // Cannot split at the very first (0) or very last frame (N-1)
        let isDisabled = true;
        let feedbackMsg = '';
        let isError = true;

        // Check if the clip is long enough to be split at all
        if (totalFrames <= (2 * minFrameMargin)) {
            feedbackMsg = `Clip too short to split (Frames: ${totalFrames})`;
        }
        // Check if selected frame is within the valid range (must be >= margin and < totalFrames - margin)
        else if (frameNumber < minFrameMargin) {
            feedbackMsg = `Cannot split at frame 0. Select frame ≥ ${minFrameMargin}.`;
        } else if (frameNumber >= (totalFrames - minFrameMargin)) {
            // Frame numbers are 0 to N-1. Max valid split frame is N-2.
            feedbackMsg = `Cannot split at last frame. Select frame ≤ ${totalFrames - minFrameMargin - 1}.`;
        } else {
            // Frame is valid
            isDisabled = false;
            feedbackMsg = 'Valid split point.';
            isError = false;
        }

        // Update button state and feedback message/style
        confirmSplitBtn.disabled = isDisabled;
        splitFeedback.textContent = feedbackMsg;
        splitFeedback.style.color = isError ? '#d9534f' : '#5cb85c'; // Red for error, Green for valid

    } // --- End of updateSplitUI ---


    // --- Keyboard Event Listeners ---
    document.addEventListener('keydown', handleGlobalKeydown);
    document.addEventListener('keyup', handleGlobalKeyup);

    /** Handles key release to update modifier/action key states */
    function handleGlobalKeyup(event) {
        const key = event.key.toLowerCase();

        if (keyState.hasOwnProperty(key)) { // Check if it's a key we track state for
            keyState[key] = false;
        }
        // Specifically update modifier keys on keyup
        if (event.metaKey === false && keyState.meta) keyState.meta = false;
        if (event.ctrlKey === false && keyState.control) keyState.control = false;

         // console.log("Key Up:", key, "State:", JSON.stringify(keyState)); // Debugging
    }

    /** Main handler for keydown events */
    async function handleGlobalKeydown(event) {
        // Ignore input if focus is inside text input, textarea etc.
        const targetTagName = event.target.tagName.toLowerCase();
        if (['input', 'textarea', 'select'].includes(targetTagName)) {
            // Allow modifier keys (Ctrl/Cmd) even in inputs, e.g., for copy/paste
             if (!event.metaKey && !event.ctrlKey) {
                return;
             }
        }

        // Ignore if no clip item exists or if it's currently processing an action
        if (!clipItem || clipItem.classList.contains('processing')) {
            // console.log("Ignoring keydown: No clip item or processing state.");
            return;
        }

        const key = event.key.toLowerCase();
        const player = window.spritePlayer;
        const viewerFocused = document.activeElement === player?.viewerElement;

        // Update modifier key states immediately on keydown
        keyState.meta = event.metaKey;
        keyState.control = event.ctrlKey;

        // Update primary action key states (A, S, D, F)
        if (keyState.hasOwnProperty(key)) {
            keyState[key] = true;
        }
         // console.log("Key Down:", key, "Meta:", keyState.meta, "Ctrl:", keyState.control, "Split Active:", splitModeActive); // Debugging

        // --- Actions Requiring Enter Key ---
        if (key === 'enter') {
            event.preventDefault(); // Prevent default form submission behavior

            const feedbackDiv = clipItem.querySelector('.action-feedback');
            let actionHandled = false; // Flag to prevent multiple actions on one Enter press

            // 1. Check Split Confirmation
            if (splitModeActive) {
                const confirmBtn = clipItem.querySelector('.confirm-split-btn');
                if (confirmBtn && !confirmBtn.disabled) {
                    console.log("[Keyboard] Enter: Confirming split...");
                    confirmBtn.disabled = true; // Prevent double submit visually
                    setProcessingState(clipItem, true); // Show processing state
                    const frame = player?.getCurrentFrame();
                    if (typeof frame === 'number') {
                         // API call handles reload on success
                         await performApiSplit(currentClipId, frame, clipItem, feedbackDiv, clipItem.querySelector('.split-controls'));
                         // If the API call fails, processing state is reset by performApiSplit
                    } else {
                         showFeedback(feedbackDiv, "Error: Could not get split frame.", true);
                         confirmBtn.disabled = false; // Re-enable on error
                         setProcessingState(clipItem, false);
                    }
                    actionHandled = true;
                }
            }

            // 2. Check Standard Actions (if split wasn't handled)
            if (!actionHandled) {
                 let actionToPerform = null;
                 if (keyState.a) actionToPerform = 'approve';
                 else if (keyState.s) actionToPerform = 'skip';
                 else if (keyState.d) actionToPerform = 'archive';
                 else if ((keyState.meta || keyState.control) && keyState.f) {
                      // Check if merge button is actually enabled before attempting
                      const mergeBtn = clipItem.querySelector('.merge-previous-btn');
                      if (mergeBtn && !mergeBtn.disabled) {
                           actionToPerform = 'merge_previous';
                      } else {
                           showFeedback(feedbackDiv, "Merge with previous is not available for this clip.", true);
                            setTimeout(() => { if(feedbackDiv) feedbackDiv.textContent = ''; }, 3000);
                            // Reset F key state so user doesn't have to release it
                            keyState.f = false;
                      }
                 }

                 if (actionToPerform) {
                     console.log(`[Keyboard] Enter: Triggering action '${actionToPerform}'`);
                     setProcessingState(clipItem, true); // Show processing
                     // API call handles reload on success
                     await performApiAction(currentClipId, actionToPerform, clipItem, feedbackDiv);
                      // If the API call fails, processing state is reset by performApiAction
                     actionHandled = true;
                 }
            }

            // Reset primary action key states after Enter is pressed, regardless of success
            // This prevents holding 'a' and repeatedly hitting Enter.
            keyState.a = false; keyState.s = false; keyState.d = false; keyState.f = false;

            if (actionHandled) return; // Stop further processing for this Enter press
        }

        // --- Handle Other Keys (Space, Arrows, Escape) ---
        // These don't require the Enter key

        // Player is needed for these controls
        if (!player || !player.meta?.isValid) {
            // Allow Escape even if player isn't ready, to cancel split mode if somehow entered
            if (key === 'escape' && splitModeActive) {
                 event.preventDefault();
                 cancelSplitMode();
            }
            return;
        }

        switch (key) {
            case ' ': // Spacebar
                // Only toggle playback if the main viewer area has focus
                if (viewerFocused) {
                    event.preventDefault(); // Prevent page scroll
                    player.togglePlayback();
                }
                break;

            case 'arrowleft':
            case 'arrowright':
                event.preventDefault(); // Prevent browser scrolling/navigation
                if (!splitModeActive) {
                    // Enter split mode automatically on first arrow press
                    activateSplitMode();
                    // If activation failed (e.g., no sprite), do nothing further
                    if (!splitModeActive) break;
                }
                 // If split mode is active (either previously or just now)
                if (splitModeActive) {
                    player.pause('arrowKey'); // Ensure paused for frame scrubbing
                    const change = (key === 'arrowleft' ? -1 : 1);
                    const newFrame = player.getCurrentFrame() + change;
                    // Update frame visually and trigger split UI update via callback
                    player.updateFrame(newFrame, true);
                }
                break;

            case 'escape':
                 if (splitModeActive) {
                     event.preventDefault(); // Prevent potential browser default actions for Escape
                     cancelSplitMode();
                 }
                 break;

            // Allow other keys (like 'a', 's', 'd', 'f') to pass through without action
            // until Enter is pressed. They just set the keyState.
            default:
                 // Optional: Log unhandled keys if needed for debugging
                 // console.log("Unhandled keydown:", key);
                 break;
        }
    } // --- End handleGlobalKeydown ---


    // --- Click Listener (Backup / Visual Aid) ---
    // Attaches to the main container for the clip item
    if (clipItem) {
        clipItem.addEventListener('click', async (event) => {
            const target = event.target;
            // Find the closest button element that is an action button
            const actionBtn = target.closest('.action-btn');

            // Ignore clicks not on an action button, disabled buttons, or while processing
            if (!actionBtn || actionBtn.disabled || clipItem.classList.contains('processing')) {
                return;
            }

            const action = actionBtn.dataset.action;
            const feedbackDiv = clipItem.querySelector('.action-feedback');
            const player = window.spritePlayer;

            if (!action) {
                 console.warn("Clicked button has no data-action defined.");
                 return;
            }

            if (player) player.pause('buttonClick'); // Pause sprite on any action click

            // Handle different button actions
            switch (action) {
                case 'approve':
                case 'skip':
                case 'archive':
                case 'retry_sprite_gen':
                case 'merge_previous':
                    setProcessingState(clipItem, true);
                    await performApiAction(currentClipId, action, clipItem, feedbackDiv);
                    break;
                case 'enter_split_mode':
                    activateSplitMode(); // Enter split mode UI
                    break;
                case 'cancel_split_mode':
                    cancelSplitMode(); // Exit split mode UI
                    break;
                case 'confirm_split':
                    const frame = player?.getCurrentFrame();
                    if (typeof frame === 'number') {
                        actionBtn.disabled = true; // Prevent double click
                        setProcessingState(clipItem, true);
                        await performApiSplit(currentClipId, frame, clipItem, feedbackDiv, clipItem.querySelector('.split-controls'));
                    } else {
                         showFeedback(feedbackDiv, "Error: Cannot get frame number to split.", true);
                    }
                    break;
                default:
                    console.log("Unknown button action clicked:", action);
            }
        });

         // Specific listener for the Play/Pause button to prevent event bubbling
         // and ensure it works correctly with the player instance.
         const playPauseBtn = clipItem.querySelector('.sprite-play-pause-btn');
         if(playPauseBtn && window.spritePlayer) {
             playPauseBtn.addEventListener('click', (e) => {
                  e.stopPropagation(); // Stop click from bubbling to the clipItem listener
                  if (!playPauseBtn.disabled) {
                       window.spritePlayer.togglePlayback();
                  }
             });
         }
    }


    // --- Cleanup on Page Unload ---
    // Ensures the player interval is stopped if the user navigates away quickly
    window.addEventListener('beforeunload', () => {
        if (window.spritePlayer) {
            console.log("[Main] Cleaning up player before page unload...");
            window.spritePlayer.cleanup();
            window.spritePlayer = null; // Clear the reference
        }
        // Remove global listeners to prevent potential memory leaks if the script were somehow kept alive
        document.removeEventListener('keydown', handleGlobalKeydown);
        document.removeEventListener('keyup', handleGlobalKeyup);
    });

}); // --- End DOMContentLoaded ---