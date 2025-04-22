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
        'a': false, 's': false, 'd': false, 'f': false, 'g': false, // Action keys
        'meta': false,    // Cmd on Mac - Still track for potential future use or other shortcuts
        'control': false, // Ctrl on Win/Linux - Still track
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

        // Cache action buttons during init
        const actionButtons = {
            approve: clipItem.querySelector('.approve-btn'),
            skip: clipItem.querySelector('.skip-btn'),
            archive: clipItem.querySelector('.archive-btn'),
            merge_previous: clipItem.querySelector('.merge-previous-btn'),
            group_previous: clipItem.querySelector('.group-previous-btn'),
        };


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
                        total_sprite_frames: parseInt(meta.total_sprite_frames, 10),
                        clip_fps: parseFloat(meta.clip_fps_source), // Use clip_fps_source
                        clip_total_frames: parseInt(meta.clip_total_frames_source, 10), // Use clip_total_frames_source
                        spriteUrl: spriteUrl
                    };

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
                     // console.log(`[Main Init ${currentClipId}] SpritePlayer initialized successfully.`);

                     // Autoplay if requested
                     if (shouldAutoplay && window.spritePlayer) {
                         // console.log(`[Main Init ${currentClipId}] Autoplaying sprite...`);
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

        // --- Helper Function to Update Button Active States ---
        function updateActiveButtonState() {
            if (!clipItem || clipItem.classList.contains('processing')) {
                return;
            }

            let activeAction = null;
            if (keyState.a) activeAction = 'approve';
            else if (keyState.s) activeAction = 'skip';
            else if (keyState.d) activeAction = 'archive';
            else if (keyState.f) activeAction = 'merge_previous';
            else if (keyState.g) activeAction = 'group_previous';

            for (const action in actionButtons) {
                const button = actionButtons[action];
                if (button) {
                    const shouldBeActive = (action === activeAction) && !button.disabled;
                    if (shouldBeActive) {
                        button.classList.add('btn-active');
                    } else {
                        button.classList.remove('btn-active');
                    }
                }
            }
        }

        // --- Split Mode UI Management Functions ---
        function activateSplitMode() {
            const player = window.spritePlayer;
            if (splitModeActive || !player || !player.meta?.isValid) {
                console.warn("Cannot activate split mode:", {splitModeActive, playerValid: !!player?.meta?.isValid});
                const feedbackDiv = clipItem?.querySelector('.action-feedback');
                if(feedbackDiv) showFeedback(feedbackDiv, "Cannot enter split mode (sprite unavailable or invalid).", true);
                 setTimeout(() => { if(feedbackDiv) feedbackDiv.textContent = ''; }, 3000);
                return;
            }

            player.pause('splitModeActivate');
            splitModeActive = true;

            const splitControls = clipItem.querySelector('.split-controls');
            const splitModeBtn = clipItem.querySelector('.split-mode-btn');
            const splitFeedback = clipItem.querySelector('.split-feedback');
            const allActionButtons = clipItem.querySelectorAll('.clip-actions > .action-btn:not(.split-mode-btn)');

            if (splitControls) splitControls.style.display = 'block';
            if (splitModeBtn) splitModeBtn.style.display = 'none';
            if (splitFeedback) splitFeedback.textContent = '';

            allActionButtons.forEach(btn => {
                 if (!btn.classList.contains('retry-sprite-btn') || btn.disabled) {
                    btn.disabled = true;
                 }
            });

            const currentFrame = player.getCurrentFrame();
            updateSplitUI(currentClipId, currentFrame, player.meta);

            player.viewerElement?.focus();
            console.log(`[Main ${currentClipId}] Split mode activated.`);
            updateActiveButtonState(); // Ensure button states reset
        }

        function cancelSplitMode() {
            if (!splitModeActive) return;
            splitModeActive = false;

            const splitControls = clipItem.querySelector('.split-controls');
            const splitModeBtn = clipItem.querySelector('.split-mode-btn');
            const allActionButtons = clipItem.querySelectorAll('.clip-actions > .action-btn:not(.split-mode-btn)');

            if (splitControls) splitControls.style.display = 'none';
            if (splitModeBtn) {
                splitModeBtn.style.display = 'inline-block';
                // Re-enable based on sprite validity, assuming it doesn't change while in split mode
                splitModeBtn.disabled = !(window.spritePlayer?.meta?.isValid);
            }

            // Re-enable buttons, respecting their *initial* disabled state from the template
            // Re-enable buttons, respecting their *initial* disabled state from the template
            allActionButtons.forEach(btn => {
                // Check if the button was likely disabled by the template's logic
                // by checking the specific 'title' attribute text.
                const isDisabledByTemplate = btn.disabled &&
                   (btn.title.includes("not available or not in an actionable state") || // Check for the combined title
                    btn.title.includes("Sprite unavailable or failed")); // Check for sprite issue title

                if (!isDisabledByTemplate) {
                    btn.disabled = false; // Only re-enable if it wasn't disabled by template logic
                }
           });

            window.spritePlayer?.viewerElement?.focus();
            console.log(`[Main ${currentClipId}] Split mode cancelled.`);
            updateActiveButtonState(); // Update button active visual state
        }

        function updateSplitUI(clipId, frameNumber, meta) {
            if (!splitModeActive || clipId !== currentClipId) return;
            if (!clipItem || !meta || !meta.isValid) {
                console.warn(`[Split UI ${clipId}] Cannot update: Missing clip item or invalid meta.`);
                return;
            }
            const confirmFrameSpan = clipItem.querySelector('.split-confirm-frame');
            const confirmTimeSpan = clipItem.querySelector('.split-confirm-time');
            const confirmFrameBtnText = clipItem.querySelector('.split-confirm-frame-btn-text');
            const confirmSplitBtn = clipItem.querySelector('.confirm-split-btn');
            const splitFeedback = clipItem.querySelector('.split-feedback');
            const totalFramesSpan = clipItem.querySelector('.split-total-frames');

            if (!confirmFrameSpan || !confirmTimeSpan || !confirmFrameBtnText || !confirmSplitBtn || !splitFeedback || !totalFramesSpan) {
                 console.warn(`[Split UI ${clipId}] Missing one or more split control elements.`);
                 return;
            }

            const totalFrames = meta.clip_total_frames;
            const fps = meta.clip_fps;
            const displayTotalFrames = totalFrames > 0 ? totalFrames - 1 : 0;
            const approxTime = (fps > 0 && totalFrames > 0 && frameNumber >=0) ? (frameNumber / fps) : 0;

            confirmFrameSpan.textContent = frameNumber;
            confirmFrameBtnText.textContent = frameNumber;
            totalFramesSpan.textContent = displayTotalFrames;
            confirmTimeSpan.textContent = approxTime.toFixed(3);

            const minFrameMargin = 1;
            let isDisabled = true;
            let feedbackMsg = '';
            let isError = true;

            if (totalFrames <= (2 * minFrameMargin)) { feedbackMsg = `Clip too short to split (Frames: ${totalFrames})`; }
            else if (frameNumber < minFrameMargin) { feedbackMsg = `Cannot split at frame 0. Select frame ≥ ${minFrameMargin}.`; }
            else if (frameNumber >= (totalFrames - minFrameMargin)) { feedbackMsg = `Cannot split at last frame. Select frame ≤ ${totalFrames - minFrameMargin - 1}.`; }
            else { isDisabled = false; feedbackMsg = 'Valid split point.'; isError = false; }

            confirmSplitBtn.disabled = isDisabled;
            splitFeedback.textContent = feedbackMsg;
            splitFeedback.style.color = isError ? '#d9534f' : '#5cb85c';
        } // --- End of updateSplitUI ---


        // --- Keyboard Event Listeners ---
        document.addEventListener('keydown', handleGlobalKeydown);
        document.addEventListener('keyup', handleGlobalKeyup);

        function handleGlobalKeyup(event) {
            const key = event.key.toLowerCase();
            let changed = false;

            if (keyState.hasOwnProperty(key)) {
                if (keyState[key]) changed = true;
                keyState[key] = false;
            }
            if (event.metaKey === false && keyState.meta) {
                keyState.meta = false; // Changed state, but doesn't directly affect merge visual anymore
            }
            if (event.ctrlKey === false && keyState.control) {
                keyState.control = false; // Changed state, but doesn't directly affect merge visual anymore
            }

            if (changed) { // Only update if A, S, D, or F was released
                updateActiveButtonState();
            }
        }

        async function handleGlobalKeydown(event) {
            const targetTagName = event.target.tagName.toLowerCase();
            if (['input', 'textarea', 'select'].includes(targetTagName)) {
                 if (!event.metaKey && !event.ctrlKey) return;
            }
            if (!clipItem || clipItem.classList.contains('processing')) return;

            const key = event.key.toLowerCase();
            const player = window.spritePlayer;
            const viewerFocused = document.activeElement === player?.viewerElement;
            let keyStateChanged = false;

            // Update modifier key states (still track them)
            if (event.metaKey !== keyState.meta) { keyState.meta = event.metaKey; }
            if (event.ctrlKey !== keyState.control) { keyState.control = event.ctrlKey; }

            // Update primary action key states
            if (keyState.hasOwnProperty(key)) {
                 if (!keyState[key]) keyStateChanged = true;
                 keyState[key] = true;
            }

            // Update Button Visual State if A,S,D, or F was pressed
            if (keyStateChanged) {
                 updateActiveButtonState();
            }

            // --- Actions Requiring Enter Key ---
            if (key === 'enter') {
                event.preventDefault();
                const feedbackDiv = clipItem.querySelector('.action-feedback');
                let actionHandled = false;

                let actionToPerform = null;
                if (keyState.a) actionToPerform = 'approve';
                else if (keyState.s) actionToPerform = 'skip';
                else if (keyState.d) actionToPerform = 'archive';
                else if (keyState.f) {
                    const mergeBtn = actionButtons['merge_previous'];
                    if (mergeBtn && !mergeBtn.disabled) {
                        actionToPerform = 'merge_previous';
                    } else {
                         showFeedback(feedbackDiv, "Merge with previous is not available.", true);
                         setTimeout(() => { if(feedbackDiv) feedbackDiv.textContent = ''; }, 3000);
                         keyState.f = false;
                         updateActiveButtonState(); // Update UI as F key is no longer effectively down
                    }
                }
                else if (keyState.g) {
                    const groupBtn = actionButtons['group_previous'];
                    if (groupBtn && !groupBtn.disabled) { actionToPerform = 'group_previous'; }
                    else {
                         showFeedback(feedbackDiv, "Group with previous is not available.", true);
                         setTimeout(() => { if(feedbackDiv) feedbackDiv.textContent = ''; }, 3000);
                         keyState.g = false; // Reset g state if button disabled
                         updateActiveButtonState();
                    }
               }

                // Handle Split confirmation FIRST if in split mode
                if (splitModeActive) {
                     const confirmBtn = clipItem.querySelector('.confirm-split-btn');
                     if (confirmBtn && !confirmBtn.disabled) {
                         console.log("[Keyboard] Enter: Confirming split...");
                         confirmBtn.disabled = true;
                         setProcessingState(clipItem, true);
                         const frame = player?.getCurrentFrame();
                         if (typeof frame === 'number') {
                              await performApiSplit(currentClipId, frame, clipItem, feedbackDiv, clipItem.querySelector('.split-controls'));
                         } else {
                              showFeedback(feedbackDiv, "Error: Could not get split frame.", true);
                              confirmBtn.disabled = false;
                              setProcessingState(clipItem, false);
                         }
                         actionHandled = true;
                     }
                // Execute standard action if split wasn't handled and an action key was held
                } else if (actionToPerform) {
                     console.log(`[Keyboard] Enter: Triggering action '${actionToPerform}'`);
                     setProcessingState(clipItem, true);
                     await performApiAction(currentClipId, actionToPerform, clipItem, feedbackDiv);
                     actionHandled = true;
                }

                // Reset primary action key states after Enter
                keyState.a = false; keyState.s = false; keyState.d = false; keyState.f = false;
                if (actionHandled) {
                     updateActiveButtonState(); // Ensure buttons deactivate after Enter completes
                     return;
                }
            }

            // --- Handle Other Keys (Space, Arrows, Escape) ---
            if (!player || !player.meta?.isValid) {
                if (key === 'escape' && splitModeActive) {
                     event.preventDefault();
                     cancelSplitMode();
                }
                return;
            }
             switch (key) {
                 case ' ':
                     if (viewerFocused) {
                         event.preventDefault();
                         player.togglePlayback();
                     }
                     break;
                 case 'arrowleft':
                 case 'arrowright':
                     event.preventDefault();
                     if (!splitModeActive) activateSplitMode();
                     if (splitModeActive) {
                         player.pause('arrowKey');
                         const change = (key === 'arrowleft' ? -1 : 1);
                         const newFrame = player.getCurrentFrame() + change;
                         player.updateFrame(newFrame, true);
                     }
                     break;
                 case 'escape':
                      if (splitModeActive) {
                          event.preventDefault();
                          cancelSplitMode();
                      }
                      break;
             }
        } // --- End handleGlobalKeydown ---

        // --- Click Listener (Backup / Visual Aid) ---
         if (clipItem) {
             clipItem.addEventListener('click', async (event) => {
                 const target = event.target;
                 const actionBtn = target.closest('.action-btn');
                 if (!actionBtn || actionBtn.disabled || clipItem.classList.contains('processing')) { return; }
                 const action = actionBtn.dataset.action;
                 const feedbackDiv = clipItem.querySelector('.action-feedback');
                 const player = window.spritePlayer;
                 if (!action) { console.warn("Clicked button has no data-action defined."); return; }
                 if (player) player.pause('buttonClick');

                 switch (action) {
                     case 'approve': case 'skip': case 'archive': case 'retry_sprite_gen': case 'merge_previous': case 'group_previous':
                         setProcessingState(clipItem, true);
                         await performApiAction(currentClipId, action, clipItem, feedbackDiv);
                         break;
                     case 'enter_split_mode': activateSplitMode(); break;
                     case 'cancel_split_mode': cancelSplitMode(); break;
                     case 'confirm_split':
                         const frame = player?.getCurrentFrame();
                         if (typeof frame === 'number') {
                             actionBtn.disabled = true; setProcessingState(clipItem, true);
                             await performApiSplit(currentClipId, frame, clipItem, feedbackDiv, clipItem.querySelector('.split-controls'));
                         } else { showFeedback(feedbackDiv, "Error: Cannot get frame number to split.", true); }
                         break;
                     default: console.log("Unknown button action clicked:", action);
                 }
             });
             const playPauseBtn = clipItem.querySelector('.sprite-play-pause-btn');
             if(playPauseBtn && window.spritePlayer) {
                 playPauseBtn.addEventListener('click', (e) => {
                      e.stopPropagation();
                      if (!playPauseBtn.disabled) { window.spritePlayer.togglePlayback(); }
                 });
             }
         }

        // --- Cleanup on Page Unload ---
        window.addEventListener('beforeunload', () => {
            if (window.spritePlayer) {
                console.log("[Main] Cleaning up player before page unload...");
                window.spritePlayer.cleanup();
                window.spritePlayer = null;
            }
            document.removeEventListener('keydown', handleGlobalKeydown);
            document.removeEventListener('keyup', handleGlobalKeyup);
        });

    } else {
        console.log("No clip review item found on the page.");
    }

}); // --- End DOMContentLoaded ---