/**
 * All scripts relating to intake_review UI, including helper functions
 * for UI feedback/state and API interactions, along with the main logic for
 * the review UI (player initialization, keyboard shortcuts, split mode).
 */

// --- Helper Functions ---

/**
 * Displays a temporary feedback message.
 * @param {HTMLElement} feedbackDiv - The DOM element to display feedback in.
 * @param {string} message - The message to display.
 * @param {boolean} [isError=false] - Whether the message represents an error.
 */
function showFeedback(feedbackDiv, message, isError = false) {
    if (!feedbackDiv) return;
    feedbackDiv.textContent = message;
    feedbackDiv.className = `action-feedback ${isError ? 'error' : 'success'}`;
    // Optional: Clear feedback after a delay
    // setTimeout(() => { if(feedbackDiv) feedbackDiv.textContent = ''; }, isError ? 5000 : 2000);
}

/**
 * Sets a processing state (visual cue) on a clip item.
 * @param {HTMLElement} clipItem - The main container element for the clip review item.
 * @param {boolean} isProcessing - Whether the item is currently being processed.
 */
function setProcessingState(clipItem, isProcessing) {
    if (!clipItem) return;
    if (isProcessing) {
        clipItem.classList.add('processing');
    } else {
        clipItem.classList.remove('processing');
    }
}

/**
 * Makes the API call for standard actions (approve, skip, archive, merge_previous, group_previous, retry_sprite_gen).
 * Reloads the page on success.
 * @param {string} clipId - The database ID of the clip.
 * @param {string} action - The action being performed.
 * @param {HTMLElement} clipItem - The clip item's container element.
 * @param {HTMLElement} feedbackDiv - The element for displaying feedback messages.
 * @returns {Promise<{success: boolean, error?: Error}>} Resolves/rejects based on API call.
 */
async function performApiAction(clipId, action, clipItem, feedbackDiv) {
    if (!clipId || !action || !clipItem || !feedbackDiv) {
         console.error("performApiAction called with missing arguments.");
         if(feedbackDiv) showFeedback(feedbackDiv, "Internal UI error.", true);
         return { success: false, error: new Error("Missing arguments") };
    }

    setProcessingState(clipItem, true);
    showFeedback(feedbackDiv, `Processing ${action}...`);

    const payload = { action: action };

    try {
        const response = await fetch(`/api/clips/${clipId}/action`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
            body: JSON.stringify(payload)
        });
        const result = await response.json(); // Try to parse JSON even on error for details

        if (!response.ok) {
            // Use detail from JSON if available, otherwise use status text
            const errorDetail = result?.detail || `HTTP error ${response.status}`;
            throw new Error(errorDetail);
        }

        showFeedback(feedbackDiv, `Success: ${action} processed. Loading next...`, false);
        // Reload the page to get the next clip
        window.location.reload();
        // Return success, although the reload interrupts further JS execution here
        return { success: true };

    } catch (error) {
        console.error(`API action '${action}' failed for ${clipId}:`, error);
        showFeedback(feedbackDiv, `Error (${action}): ${error.message}`, true);
        setProcessingState(clipItem, false); // Re-enable interaction on error
        return { success: false, error: error };
    }
}

/**
 * Makes the API call to queue a split action.
 * Reloads the page on success.
 * @param {string} clipId - The database ID of the clip.
 * @param {number} frame - The frame number at which to split the clip.
 * @param {HTMLElement} clipItem - The clip item's container element.
 * @param {HTMLElement} feedbackDiv - The main feedback element for the clip item.
 * @param {HTMLElement} splitControlsElement - The container for the split controls UI.
 * @returns {Promise<{success: boolean, error?: Error}>} Resolves/rejects based on API call.
 */
async function performApiSplit(clipId, frame, clipItem, feedbackDiv, splitControlsElement) {
     if (!clipId || typeof frame !== 'number' || !clipItem || !feedbackDiv || !splitControlsElement) {
        console.error("performApiSplit called with missing arguments.");
        if(feedbackDiv) showFeedback(feedbackDiv, "Internal UI error.", true);
        return { success: false, error: new Error("Missing arguments") };
    }

    setProcessingState(clipItem, true);
    showFeedback(feedbackDiv, 'Queueing split...');
    const splitFeedback = splitControlsElement.querySelector('.split-feedback');
    if (splitFeedback) splitFeedback.textContent = 'Queueing split...';

    const payload = { split_request_at_frame: frame };

    try {
        const response = await fetch(`/api/clips/${clipId}/split`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
            body: JSON.stringify(payload)
        });
        const result = await response.json(); // Try parsing on error too

        if (!response.ok) {
             const errorDetail = result?.detail || `HTTP error ${response.status}`;
             throw new Error(errorDetail);
        }

        showFeedback(feedbackDiv, 'Success: Split queued. Loading next...', false);
        // Reload the page
        window.location.reload();
        return { success: true };

    } catch (error) {
        console.error(`Split API failed for clip ${clipId}:`, error);
        const errorMsg = `Split Error: ${error.message}`;
        showFeedback(feedbackDiv, errorMsg, true);
        if (splitFeedback) {
             splitFeedback.textContent = errorMsg;
             splitFeedback.style.color = '#d00';
         }
        setProcessingState(clipItem, false); // Re-enable on error
        return { success: false, error: error };
    }
}


// --- Main Review UI Logic ---

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
        // Unused currently but still track for potential future use in shortcuts
        'meta': false,    // Cmd on Mac
        'control': false, // Ctrl on Win/Linux
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
            retry_sprite_gen: retryBtn // Use the reference already obtained
        };


        if (viewer) {
             viewer.textContent = 'Initializing sprite...'; // Initial message

             // Attempt to parse metadata and initialize player
             if (spriteUrl && spriteMetaJson) {
                 try {
                     let parsedOnce = JSON.parse(spriteMetaJson);
                     meta = (typeof parsedOnce === 'string') ? JSON.parse(parsedOnce) : parsedOnce;

                     if (!meta || typeof meta !== 'object') {
                         throw new Error("Parsed metadata is not a valid object.");
                     }

                     const parsedMeta = {
                        tile_width: parseFloat(meta.tile_width),
                        tile_height_calculated: parseFloat(meta.tile_height_calculated),
                        cols: parseInt(meta.cols, 10),
                        rows: parseInt(meta.rows, 10),
                        total_sprite_frames: parseInt(meta.total_sprite_frames, 10),
                        clip_fps: parseFloat(meta.clip_fps_source),
                        clip_total_frames: parseInt(meta.clip_total_frames_source, 10),
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
                     parsedMeta.isValid = true;

                     // Assume SpritePlayer class is defined/loaded elsewhere
                     window.spritePlayer = new SpritePlayer(currentClipId, viewer, scrubBar, playPauseBtn, frameDisplay, parsedMeta, updateSplitUI);

                     if (shouldAutoplay && window.spritePlayer) {
                         setTimeout(() => window.spritePlayer?.play('autoplay'), 100);
                     }
                     setTimeout(() => viewer.focus(), 150);

                 } catch (e) {
                     console.error(`[Main Init ${currentClipId}] Failed during player initialization:`, e);
                     initError = `Error initializing sprite: ${e.message}`;
                     viewer.textContent = initError;
                     viewer.classList.add('no-sprite');
                 }
             } else {
                 const spriteErrorMsg = clipItem.querySelector('.clip-info strong[style*="color:orange"]');
                 initError = spriteErrorMsg ? 'Sprite generation failed (backend).' : 'Sprite sheet unavailable.';
                 viewer.textContent = initError;
                 viewer.classList.add('no-sprite');
             }

             if (initError) {
                 console.warn(`[Main Init ${currentClipId}] Disabling controls due to: ${initError}`);
                 if (scrubBar) scrubBar.disabled = true;
                 if (playPauseBtn) playPauseBtn.disabled = true;
                 const controlsDiv = document.getElementById(`controls-${currentClipId}`);
                 if (controlsDiv) controlsDiv.style.opacity = '0.6';

                 if (splitBtn) {
                     splitBtn.disabled = true;
                     splitBtn.title = "Cannot split: Sprite unavailable or failed.";
                 }
                 if (retryBtn && (initError.includes('failed') || initError.includes('Error initializing sprite'))) {
                     retryBtn.disabled = false;
                     actionButtons.retry_sprite_gen = retryBtn;
                 } else if (retryBtn) {
                     retryBtn.disabled = true;
                 }
             } else {
                 if (retryBtn) retryBtn.disabled = true;
             }

        } else {
            console.error("Sprite viewer element (#sprite-viewer-...) not found in the DOM!");
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
            updateActiveButtonState();
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
                splitModeBtn.disabled = !(window.spritePlayer?.meta?.isValid);
            }

            allActionButtons.forEach(btn => {
                const isDisabledByInitLogic = btn.disabled && (
                   btn.title.includes("not available") ||
                   btn.title.includes("not in an actionable state") ||
                   btn.title.includes("Sprite unavailable or failed") ||
                   (btn.classList.contains('retry-sprite-btn') && !actionButtons.retry_sprite_gen)
                );

                if (!isDisabledByInitLogic) {
                    btn.disabled = false;
                }
           });

            window.spritePlayer?.viewerElement?.focus();
            console.log(`[Main ${currentClipId}] Split mode cancelled.`);
            updateActiveButtonState();
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

            if (totalFrames <= (2 * minFrameMargin)) {
                feedbackMsg = `Clip too short to split (Frames: 0-${displayTotalFrames})`;
            } else if (frameNumber < minFrameMargin) {
                 feedbackMsg = `Cannot split at frame 0. Select frame ≥ ${minFrameMargin}.`;
            } else if (frameNumber >= (totalFrames - minFrameMargin)) {
                 const maxValidFrame = totalFrames - minFrameMargin - 1;
                 feedbackMsg = `Cannot split at last frame (${displayTotalFrames}). Select frame ≤ ${maxValidFrame}.`;
            } else {
                 isDisabled = false;
                 feedbackMsg = 'Valid split point.';
                 isError = false;
            }

            confirmSplitBtn.disabled = isDisabled;
            splitFeedback.textContent = feedbackMsg;
            splitFeedback.style.color = isError ? '#d9534f' : '#5cb85c';
        }


        // --- Keyboard Event Listeners ---
        // Use named functions for easier removal on cleanup
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

            if (event.metaKey !== keyState.meta) { keyState.meta = event.metaKey; }
            if (event.ctrlKey !== keyState.control) { keyState.control = event.ctrlKey; }

            if (keyState.hasOwnProperty(key)) {
                 if (!keyState[key]) keyStateChanged = true;
                 keyState[key] = true;
            }

            if (keyStateChanged) {
                 updateActiveButtonState();
            }

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
                         updateActiveButtonState();
                    }
                }
                else if (keyState.g) {
                    const groupBtn = actionButtons['group_previous'];
                    if (groupBtn && !groupBtn.disabled) {
                        actionToPerform = 'group_previous';
                    } else {
                         showFeedback(feedbackDiv, "Group with previous is not available.", true);
                         setTimeout(() => { if(feedbackDiv) feedbackDiv.textContent = ''; }, 3000);
                         keyState.g = false;
                         updateActiveButtonState();
                    }
               }

                if (splitModeActive) {
                     const confirmBtn = clipItem.querySelector('.confirm-split-btn');
                     if (confirmBtn && !confirmBtn.disabled) {
                         console.log("[Keyboard] Enter: Confirming split...");
                         confirmBtn.disabled = true;
                         const frame = player?.getCurrentFrame();
                         if (typeof frame === 'number') {
                              await performApiSplit(currentClipId, frame, clipItem, feedbackDiv, clipItem.querySelector('.split-controls'));
                         } else {
                              showFeedback(feedbackDiv, "Error: Could not get split frame.", true);
                              confirmBtn.disabled = false;
                         }
                         actionHandled = true;
                     }
                } else if (actionToPerform) {
                     console.log(`[Keyboard] Enter: Triggering action '${actionToPerform}'`);
                     await performApiAction(currentClipId, actionToPerform, clipItem, feedbackDiv);
                     actionHandled = true;
                }

                keyState.a = false; keyState.s = false; keyState.d = false; keyState.f = false; keyState.g = false;
                if (actionHandled) {
                     updateActiveButtonState();
                     return;
                }
            }

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
        }

        function handleGlobalKeyup(event) {
            const key = event.key.toLowerCase();
            let changed = false;

            if (keyState.hasOwnProperty(key)) {
                if (keyState[key]) changed = true;
                keyState[key] = false;
            }
            if (event.metaKey === false && keyState.meta) {
                keyState.meta = false;
            }
            if (event.ctrlKey === false && keyState.control) {
                keyState.control = false;
            }

            if (changed) {
                updateActiveButtonState();
            }
        }

        document.addEventListener('keydown', handleGlobalKeydown);
        document.addEventListener('keyup', handleGlobalKeyup);


        // --- Click Listener ---
         if (clipItem) {
             clipItem.addEventListener('click', async (event) => {
                 const target = event.target;
                 const actionBtn = target.closest('.action-btn');

                 if (!actionBtn || actionBtn.disabled || clipItem.classList.contains('processing')) {
                     if (!target.closest('.sprite-play-pause-btn')) {
                          return;
                     }
                 }

                 const action = actionBtn.dataset.action;
                 const feedbackDiv = clipItem.querySelector('.action-feedback');
                 const player = window.spritePlayer;

                 if (player && action !== 'toggle_play_pause') {
                    player.pause('buttonClick');
                 }

                 if (!action) {
                    if (!target.closest('.sprite-play-pause-btn')) {
                         console.warn("Clicked button has no data-action defined:", actionBtn);
                    }
                    return;
                 }

                 switch (action) {
                     case 'approve':
                     case 'skip':
                     case 'archive':
                     case 'retry_sprite_gen':
                     case 'merge_previous':
                     case 'group_previous':
                         await performApiAction(currentClipId, action, clipItem, feedbackDiv);
                         break;
                     case 'enter_split_mode':
                         activateSplitMode();
                         break;
                     case 'cancel_split_mode':
                         cancelSplitMode();
                         break;
                     case 'confirm_split':
                         const frame = player?.getCurrentFrame();
                         if (typeof frame === 'number') {
                             actionBtn.disabled = true;
                             await performApiSplit(currentClipId, frame, clipItem, feedbackDiv, clipItem.querySelector('.split-controls'));
                         } else {
                             showFeedback(feedbackDiv, "Error: Cannot get frame number to split.", true);
                         }
                         break;
                     default:
                         console.log("Unknown button action clicked:", action);
                 }
             });

             const playPauseBtnInstance = clipItem.querySelector('.sprite-play-pause-btn');
             if(playPauseBtnInstance && window.spritePlayer) {
                 playPauseBtnInstance.addEventListener('click', (e) => {
                      e.stopPropagation();
                      if (!playPauseBtnInstance.disabled && window.spritePlayer) {
                          window.spritePlayer.togglePlayback();
                      }
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
            console.log("[Main] Removed global key listeners.");
        });

    } else {
        console.log("No clip review item (.clip-review-item) found on the page.");
    }

});