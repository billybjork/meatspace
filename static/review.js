// --- START OF FILE static/review.js ---
document.addEventListener('DOMContentLoaded', () => {
    const reviewQueue = document.getElementById('review-queue');
    const spriteViewers = {}; // Store state for each viewer/clip

    // --- Helper Functions ---

    function calculateBGPosition(clipId, frameNumber) {
        const viewerState = spriteViewers[clipId];
        // Early exit if state or meta is missing/invalid
        if (!viewerState || !viewerState.meta || !viewerState.meta.isValid) {
            // console.warn(`[calculateBGPosition] Invalid meta or state for clip ${clipId}`);
            return { bgX: 0, bgY: 0 }; // Default position
        }
        const meta = viewerState.meta;

        // Clamp frame number
        frameNumber = Math.max(0, Math.min(frameNumber, meta.clip_total_frames - 1));

        // Proportional mapping: Find where the current frame lies proportionally within the clip,
        // then map that proportion to the available sprite frames.
        const proportionThroughClip = (meta.clip_total_frames > 1) ? (frameNumber / (meta.clip_total_frames - 1)) : 0; // Use -1 for 0-based index max
        const targetSpriteFrame = proportionThroughClip * (meta.total_sprite_frames - 1); // Map to 0-based sprite index max
        const spriteFrameIndex = Math.max(0, Math.min(Math.floor(targetSpriteFrame), meta.total_sprite_frames - 1));

        const col = spriteFrameIndex % meta.cols;
        const row = Math.floor(spriteFrameIndex / meta.cols);

        // Ensure tile dimensions are numbers
        const tileW = parseFloat(meta.tile_width);
        const tileH = parseFloat(meta.tile_height_calculated);

        if (isNaN(tileW) || isNaN(tileH) || tileW <=0 || tileH <= 0) {
             console.error(`[calculateBGPosition] Invalid tile dimensions for clip ${clipId}: W=${tileW}, H=${tileH}`);
             return { bgX: 0, bgY: 0 };
        }

        const bgX = -(col * tileW);
        const bgY = -(row * tileH);

        // console.debug(`Frame: ${frameNumber}, Prop: ${proportionThroughClip.toFixed(3)}, TargetSprite: ${targetSpriteFrame.toFixed(3)}, Index: ${spriteFrameIndex}, Col: ${col}, Row: ${row}, Pos: ${bgX}px ${bgY}px`);

        return { bgX, bgY };
    }


    function updateSpriteFrame(clipId, frameNumber) {
        const clipItem = document.getElementById(`clip-${clipId}`);
        if (!clipItem) return; // Exit if clip element is gone

        const viewerState = spriteViewers[clipId];
        // Early exit if state or meta is missing/invalid
        if (!viewerState || !viewerState.meta || !viewerState.meta.isValid) {
             // console.warn(`[updateSpriteFrame] Invalid meta or state for clip ${clipId}`);
             return;
        }

        const viewer = document.getElementById(`sprite-viewer-${clipId}`);
        const frameDisplay = document.getElementById(`frame-display-${clipId}`);
        const scrubBar = document.getElementById(`scrub-${clipId}`);
        const meta = viewerState.meta;


        // Clamp frame number using valid total frames
        const maxFrame = meta.clip_total_frames > 0 ? meta.clip_total_frames - 1 : 0;
        frameNumber = Math.max(0, Math.min(frameNumber, maxFrame));
        frameNumber = Math.round(frameNumber); // Ensure integer

        viewerState.currentFrame = frameNumber;

        // Calculate and set background position
        const { bgX, bgY } = calculateBGPosition(clipId, frameNumber);
        if (viewer) {
             viewer.style.backgroundPosition = `${bgX}px ${bgY}px`;
        } else {
             // console.warn("Sprite viewer element not found for update:", clipId);
             return; // Stop if viewer gone
        }


        if (frameDisplay) frameDisplay.textContent = `Frame: ${frameNumber}`;
        // Avoid updating scrub bar if it's the element currently being interacted with
        if (scrubBar && document.activeElement !== scrubBar) {
            scrubBar.value = frameNumber;
        }

        // If in split mode, update split controls
        if (viewerState.isSplitMode) {
            updateSplitControls(clipId, frameNumber);
        }
    }

     function updateSplitControls(clipId, frameNumber) {
         const clipItem = document.getElementById(`clip-${clipId}`);
         if (!clipItem) return;

         const viewerState = spriteViewers[clipId];
         // Early exit if state or meta is missing/invalid
         if (!viewerState || !viewerState.meta || !viewerState.meta.isValid) {
              console.warn(`[updateSplitControls] Invalid meta or state for clip ${clipId}`);
              return;
         }
        const meta = viewerState.meta;

        const confirmFrameSpan = clipItem.querySelector('.split-confirm-frame');
        const confirmTimeSpan = clipItem.querySelector('.split-confirm-time');
        const confirmFrameBtnText = clipItem.querySelector('.split-confirm-frame-btn-text');
        const confirmSplitBtn = clipItem.querySelector('.confirm-split-btn');
        const splitFeedback = clipItem.querySelector('.split-feedback');
        const totalFramesSpan = clipItem.querySelector('.split-total-frames');

        if (!confirmFrameSpan || !confirmTimeSpan || !confirmFrameBtnText || !confirmSplitBtn || !splitFeedback || !totalFramesSpan) {
            console.error("Split control elements not found for clip:", clipId);
            return;
        }

        const totalFrames = meta.clip_total_frames;
        const fps = meta.clip_fps; // Already validated to be > 0 if meta.isValid

        confirmFrameSpan.textContent = frameNumber;
        confirmFrameBtnText.textContent = frameNumber;
        totalFramesSpan.textContent = totalFrames > 0 ? totalFrames - 1 : '?'; // Max frame index

        const approxTime = (fps > 0 && totalFrames > 0) ? (frameNumber / fps) : 0;
        confirmTimeSpan.textContent = approxTime.toFixed(3);

        // Validation logic
        const minFrameMargin = 1; // Need at least 1 frame before and 1 after
        let isDisabled = true;
        let feedbackMsg = '';

        if (totalFrames <= (2 * minFrameMargin)) { // Check if clip is too short to split
            feedbackMsg = `Clip too short to split (Total Frames: ${totalFrames})`;
        } else if (frameNumber < minFrameMargin) {
            feedbackMsg = `Select frame ≥ ${minFrameMargin}.`;
        } else if (frameNumber >= (totalFrames - minFrameMargin)) {
            feedbackMsg = `Select frame ≤ ${totalFrames - minFrameMargin - 1}.`;
        } else {
            isDisabled = false; // Enable if checks pass
            feedbackMsg = 'Valid split point.'; // Provide positive feedback
        }

        confirmSplitBtn.disabled = isDisabled;
        splitFeedback.textContent = feedbackMsg;
        splitFeedback.style.color = isDisabled ? '#d00' : '#080'; // Red if invalid, Green if valid
    }


    function togglePlayback(clipId) {
         const viewerState = spriteViewers[clipId];
         // Early exit if state or meta is missing/invalid
         if (!viewerState || !viewerState.meta || !viewerState.meta.isValid) {
              console.warn(`[togglePlayback] Invalid meta or state for clip ${clipId}`);
              return;
         }
         const meta = viewerState.meta;
         const playPauseBtn = document.querySelector(`#clip-${clipId} .sprite-play-pause-btn`);

         // FPS already validated if meta.isValid is true

         if (viewerState.isPlaying) {
             clearInterval(viewerState.playbackInterval);
             viewerState.isPlaying = false;
             if(playPauseBtn) playPauseBtn.textContent = '⏯️';
             console.log(`Clip ${clipId}: Playback Paused`);
         } else {
             viewerState.isPlaying = true;
             if(playPauseBtn) playPauseBtn.textContent = '⏸️';
             // Ensure interval time is a positive number
             const intervalTime = (meta.clip_fps > 0) ? (1000 / meta.clip_fps) : 100; // Fallback interval
             console.log(`Clip ${clipId}: Playback Started (Interval: ${intervalTime.toFixed(2)}ms, FPS: ${meta.clip_fps})`);

             viewerState.playbackInterval = setInterval(() => {
                 // Check if viewer state still exists before updating
                 if (!spriteViewers[clipId] || !spriteViewers[clipId].meta || !spriteViewers[clipId].meta.isValid) {
                     clearInterval(viewerState.playbackInterval);
                     console.log(`Stopping interval for clip ${clipId} - state invalid or removed.`);
                     return;
                 }
                 let nextFrame = spriteViewers[clipId].currentFrame + 1;
                 if (nextFrame >= spriteViewers[clipId].meta.clip_total_frames) {
                     nextFrame = 0; // Loop
                 }
                 updateSpriteFrame(clipId, nextFrame);
             }, intervalTime);
         }
    }
    // --- End Helper Functions ---

    // --- Initialize Sprite Viewers ---
    document.querySelectorAll('.clip-review-item').forEach(clipItem => {
        const clipId = clipItem.dataset.clipId;
        const spriteUrl = clipItem.dataset.spriteUrl;
        const spriteMetaJson = clipItem.dataset.spriteMeta; // This holds the JSON *string* from the attribute
        const viewer = document.getElementById(`sprite-viewer-${clipId}`);
        const scrubBar = document.getElementById(`scrub-${clipId}`);
        const playPauseBtn = clipItem.querySelector('.sprite-play-pause-btn');
        const controlsDiv = document.getElementById(`controls-${clipId}`);

        let meta = null; // Will hold the final parsed *object*
        let initError = null;

        if (viewer) viewer.textContent = 'Initializing...';

        if (spriteUrl && spriteMetaJson && viewer) {
            // console.log(`[Init ${clipId}] Initializing. Raw meta string from data attribute:`, spriteMetaJson); // Reduced logging
            try {
                // --- Double Parse Logic ---
                let parsedOnce = JSON.parse(spriteMetaJson);
                if (typeof parsedOnce === 'string') {
                    // console.warn(`[Init ${clipId}] Detected string after first parse. Attempting SECOND parse.`);
                    meta = JSON.parse(parsedOnce);
                } else {
                    meta = parsedOnce;
                }
                // --- End Double Parse ---

                // --- Validation Block ---
                if (!meta || typeof meta !== 'object') { throw new Error("Metadata did not resolve to a valid object after parsing."); }
                // console.log(`[Init ${clipId}] Passed final object check.`); // Reduced logging

                const parsedMeta = {
                    tile_width: parseFloat(meta.tile_width),
                    tile_height_calculated: parseFloat(meta.tile_height_calculated),
                    cols: parseInt(meta.cols, 10),
                    rows: parseInt(meta.rows, 10),
                    total_sprite_frames: parseInt(meta.total_sprite_frames, 10),
                    clip_fps: parseFloat(meta.clip_fps),
                    clip_total_frames: parseInt(meta.clip_total_frames, 10)
                };
                // console.log(`[Init ${clipId}] Parsed numeric properties:`, parsedMeta); // Reduced logging

                if (isNaN(parsedMeta.tile_width) || parsedMeta.tile_width <= 0) throw new Error(`Invalid tile_width: ${meta.tile_width}`);
                if (isNaN(parsedMeta.tile_height_calculated) || parsedMeta.tile_height_calculated <= 0) throw new Error(`Invalid tile_height_calculated: ${meta.tile_height_calculated}`);
                if (isNaN(parsedMeta.cols) || parsedMeta.cols <= 0) throw new Error(`Invalid cols: ${meta.cols}`);
                if (isNaN(parsedMeta.rows) || parsedMeta.rows <= 0) throw new Error(`Invalid rows: ${meta.rows}`);
                if (isNaN(parsedMeta.total_sprite_frames) || parsedMeta.total_sprite_frames <= 0) throw new Error(`Invalid total_sprite_frames: ${meta.total_sprite_frames}`);
                if (isNaN(parsedMeta.clip_fps) || parsedMeta.clip_fps <= 0) throw new Error(`Invalid clip_fps: ${meta.clip_fps}`);
                if (isNaN(parsedMeta.clip_total_frames) || parsedMeta.clip_total_frames <= 0) throw new Error(`Invalid clip_total_frames: ${meta.clip_total_frames}`);
                // --- End Validation ---

                parsedMeta.isValid = true; // Mark metadata as validated
                // console.log(`[Init ${clipId}] Meta VALIDATED:`, parsedMeta); // Reduced logging

                spriteViewers[clipId] = {
                    meta: parsedMeta, // Store the validated/parsed object
                    currentFrame: 0,
                    isPlaying: false,
                    playbackInterval: null,
                    isSplitMode: false,
                    isFocusedForKeys: false
                };

                // --- Setup UI ---
                viewer.style.backgroundImage = `url('${spriteUrl}')`;
                const bgWidth = parsedMeta.cols * parsedMeta.tile_width;
                const bgHeight = parsedMeta.rows * parsedMeta.tile_height_calculated;
                viewer.style.backgroundSize = `${bgWidth}px ${bgHeight}px`; // Size of the whole sheet

                // **** MOVED HERE: Set viewer dimensions AFTER validation ****
                viewer.style.width = `${parsedMeta.tile_width}px`; // Set width to tile width
                viewer.style.height = `${parsedMeta.tile_height_calculated}px`; // Set height to tile height
                // viewer.style.aspectRatio = `${parsedMeta.tile_width} / ${parsedMeta.tile_height_calculated}`; // Optional

                viewer.textContent = ''; // Clear "Initializing..."

                updateSpriteFrame(clipId, 0); // Show first frame

                // Setup controls
                if (scrubBar) {
                    scrubBar.max = parsedMeta.clip_total_frames - 1;
                    scrubBar.disabled = false;
                    scrubBar.addEventListener('input', (e) => {
                         const state = spriteViewers[clipId];
                         if (!state || !state.meta || !state.meta.isValid) return;
                         const frame = parseInt(e.target.value, 10);
                         if (state.isPlaying) togglePlayback(clipId);
                         updateSpriteFrame(clipId, frame); });
                }
                 if (playPauseBtn) {
                     playPauseBtn.disabled = false;
                     playPauseBtn.addEventListener('click', () => togglePlayback(clipId));
                 }
                 if(controlsDiv) controlsDiv.style.opacity = '1';

                 // Focus handling
                 viewer.setAttribute('tabindex', '0');
                 viewer.addEventListener('focus', ()=>{
                     const state = spriteViewers[clipId];
                     if(state) state.isFocusedForKeys = true;
                     viewer.style.outline = '2px solid dodgerblue';
                 });
                 viewer.addEventListener('blur', ()=>{
                    const state = spriteViewers[clipId];
                    if(state) state.isFocusedForKeys = false;
                    viewer.style.outline = 'none';
                });

            } catch (e) {
                // Catch parsing or validation errors
                console.error(`[Init ${clipId}] Failed during initialization or validation:`, e);
                initError = `Error initializing sprite: ${e.message}`;
                if(viewer) {
                     viewer.textContent = initError;
                     viewer.classList.add('no-sprite');
                }
            }
        } else {
            // Handle missing URL or metadata attribute
             initError = 'Sprite sheet URL or metadata attribute missing.';
             if(viewer) {
                 viewer.textContent = initError;
                 viewer.classList.add('no-sprite');
             }
        }

        // Final error handling / disabling controls
        if (initError) {
             console.error(`[Init ${clipId}] Initialization failed: ${initError}`);
             if (scrubBar) scrubBar.disabled = true;
             if (playPauseBtn) playPauseBtn.disabled = true;
             if (controlsDiv) controlsDiv.style.opacity = '0.5';
             // Also disable split button if init failed
             const splitBtn = clipItem.querySelector('.split-mode-btn');
             if(splitBtn) splitBtn.disabled = true;
        }
        // Handle case where sprite gen might have been skipped (but no error)
        else if (!spriteUrl && viewer) {
            const spriteErrorMsg = clipItem.querySelector('.clip-info strong[style*="color:orange"]');
            if (spriteErrorMsg) { /* Already handled if sprite_error flag passed from backend */ }
            else { viewer.textContent = 'Sprite sheet not generated (e.g., clip too short).'; }
            viewer.classList.add('no-sprite');
            if (scrubBar) scrubBar.disabled = true;
            if (playPauseBtn) playPauseBtn.disabled = true;
            if (controlsDiv) controlsDiv.style.opacity = '0.5';
             const splitBtn = clipItem.querySelector('.split-mode-btn');
             if(splitBtn) splitBtn.disabled = true;
        }
    }); // End of initialization loop

    // --- Event Listener for Actions (Delegated) ---
    reviewQueue.addEventListener('click', async (event) => {
        const target = event.target;
        const clipItem = target.closest('.clip-review-item');
        if (!clipItem) return;

        const clipId = clipItem.dataset.clipId;
        const viewerState = spriteViewers[clipId]; // May be undefined if init failed
        const feedbackDiv = clipItem.querySelector('.action-feedback');
        const undoButton = clipItem.querySelector('.undo-button');
        const splitControls = clipItem.querySelector('.split-controls');
        const splitModeBtn = clipItem.querySelector('.split-mode-btn');
        const confirmSplitBtn = clipItem.querySelector('.confirm-split-btn');
        const cancelSplitBtn = clipItem.querySelector('.cancel-split-btn');
        const splitFeedbackSpan = clipItem.querySelector('.split-feedback');
        const viewer = document.getElementById(`sprite-viewer-${clipId}`);


        let action = null;
        let payload = {};

        // --- Handle Split Mode Activation ---
        if (target.matches('.split-mode-btn')) {
            // Re-check if meta is valid before entering split mode
            if (!viewerState || !viewerState.meta || !viewerState.meta.isValid) {
                console.warn("Cannot enter split mode: Invalid sprite metadata for clip", clipId);
                alert("Cannot enter split mode: Sprite data is invalid or missing."); // User feedback
                return;
            }
            action = target.dataset.action;
            console.log(`Action: ${action} on Clip ID: ${clipId}`);
            if (viewerState.isPlaying) togglePlayback(clipId);
            viewerState.isSplitMode = true;
            splitControls.style.display = 'block';
            splitModeBtn.style.display = 'none';
            splitFeedbackSpan.textContent = ''; // Clear feedback on entry
            updateSplitControls(clipId, viewerState.currentFrame);
            if(viewer) viewer.focus();
            return;
        }

        // --- Handle Split Mode Cancellation ---
        if (target.matches('.cancel-split-btn')) {
            action = target.dataset.action;
            console.log(`Action: ${action} on Clip ID: ${clipId}`);
            if (viewerState) viewerState.isSplitMode = false;
            splitControls.style.display = 'none';
            splitModeBtn.style.display = 'inline-block';
            splitFeedbackSpan.textContent = ''; // Clear feedback on cancel
             if(viewer) viewer.focus(); // Refocus viewer after cancel? Optional.
            return;
        }

        // --- Handle Split Confirmation ---
        if (target.matches('.confirm-split-btn')) {
             if (!viewerState || !viewerState.meta || !viewerState.meta.isValid) {
                 console.error("Cannot confirm split: viewer state or meta invalid for clip", clipId);
                 splitFeedbackSpan.textContent = 'Error: Internal state invalid.';
                 return;
             }
             action = target.dataset.action;
             const splitFrame = viewerState.currentFrame;
             if (splitFrame === undefined || isNaN(splitFrame)) {
                 splitFeedbackSpan.textContent = `Error: Invalid frame number (${splitFrame}).`;
                 return;
             }
            console.log(`Action: ${action} on Clip ID: ${clipId} at frame: ${splitFrame}`);
            payload = { split_request_at_frame: splitFrame };

            // --- (Existing fetch logic - keep as is) ---
            clipItem.classList.add('processing');
            splitFeedbackSpan.textContent = 'Queueing split...';
            feedbackDiv.textContent = '';
            try {
                const response = await fetch(`/api/clips/${clipId}/split`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' }, body: JSON.stringify(payload)});
                const result = await response.json();
                if (!response.ok) throw new Error(result.detail || `HTTP error! status: ${response.status}`);
                feedbackDiv.textContent = `Success: Queued for splitting (state: ${result.new_state}). Refresh page.`;
                feedbackDiv.className = 'action-feedback success';
                clipItem.classList.remove('processing');
                clipItem.classList.add('done'); // Mark visually done
                splitControls.style.display = 'none';
                if (viewerState) viewerState.isSplitMode = false;
                if (viewerState && viewerState.playbackInterval) clearInterval(viewerState.playbackInterval);
                delete spriteViewers[clipId]; // Clean up state for completed item

            } catch (error) {
                 console.error('Split action failed:', error);
                 splitFeedbackSpan.textContent = `Error: ${error.message}`;
                 feedbackDiv.textContent = `Split Error: ${error.message}`;
                 feedbackDiv.className = 'action-feedback error';
                 clipItem.classList.remove('processing');
            }
            return;
        }

        // --- Handle standard action buttons ---
        if (target.matches('.action-btn') && !target.matches('.split-mode-btn') && !target.matches('.confirm-split-btn') && !target.matches('.cancel-split-btn')) {
            action = target.dataset.action;
            payload.action = action;
            console.log(`Action: ${action} on Clip ID: ${clipId}`);

            clipItem.classList.add('processing');
            feedbackDiv.textContent = 'Processing...';
            feedbackDiv.className = 'action-feedback';
            undoButton.style.display = 'none';

            if (viewerState && viewerState.isPlaying) togglePlayback(clipId);

            // --- (Existing fetch logic - keep as is) ---
             try {
                const response = await fetch(`/api/clips/${clipId}/action`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' }, body: JSON.stringify(payload)});
                const result = await response.json();
                if (!response.ok) throw new Error(result.detail || `HTTP error! status: ${response.status}`);
                feedbackDiv.textContent = `Success: Marked as ${result.new_state}.`;
                 if (action === 'approve' || action === 'archive') {
                     feedbackDiv.textContent += ' Item will disappear on refresh.';
                 }
                 feedbackDiv.className = 'action-feedback success';
                 clipItem.classList.remove('processing');
                 clipItem.classList.add('done');
                 if (action !== 'approve' && action !== 'archive') {
                     undoButton.style.display = 'inline-block';
                      setTimeout(() => {
                          // Check if still 'done' before hiding (might have been undone)
                          if (clipItem.classList.contains('done') && undoButton.style.display !== 'none') {
                             undoButton.style.display = 'none';
                         }
                       }, 15000);
                 }
                 if (viewerState && viewerState.playbackInterval) clearInterval(viewerState.playbackInterval);
                 // Clean up state if done and not skipped/retry
                 if (action !== 'skip' && action !== 'retry_sprite_gen') {
                     delete spriteViewers[clipId];
                 }

            } catch (error) {
                 console.error('Action failed:', error);
                 feedbackDiv.textContent = `Error: ${error.message}`;
                 feedbackDiv.className = 'action-feedback error';
                 clipItem.classList.remove('processing');
            }
             return;
        }

        // --- Handle Undo Button ---
        if (target.matches('.undo-button')) {
            console.log(`Undo Action on Clip ID: ${clipId}`);
            clipItem.classList.add('processing');
            feedbackDiv.textContent = 'Undoing...';
            feedbackDiv.className = 'action-feedback';
            undoButton.style.display = 'none';
            if (viewerState && viewerState.isPlaying) togglePlayback(clipId);

            // --- (Existing fetch logic - keep as is) ---
             try {
                 const response = await fetch(`/api/clips/${clipId}/undo`, { method: 'POST', headers: { 'Accept': 'application/json' } });
                 const result = await response.json();
                 if (!response.ok) throw new Error(result.detail || `HTTP error! status: ${response.status}`);
                 feedbackDiv.textContent = `Success: Reverted to ${result.new_state}. Refresh page.`;
                 feedbackDiv.className = 'action-feedback success';
                 clipItem.classList.remove('processing', 'done');
                 if (viewerState && viewerState.playbackInterval) clearInterval(viewerState.playbackInterval);
                 delete spriteViewers[clipId]; // Clean up state on undo

            } catch (error) {
                 console.error('Undo failed:', error);
                 feedbackDiv.textContent = `Undo Error: ${error.message}`;
                 feedbackDiv.className = 'action-feedback error';
                 clipItem.classList.remove('processing');
                 undoButton.style.display = 'inline-block';
            }
             return;
        }
    }); // End of main click listener

     // --- Keyboard Listener for Focused Viewer ---
     document.addEventListener('keydown', (event) => {
         const focusedElement = document.activeElement;
         if (!focusedElement || !focusedElement.classList.contains('sprite-viewer')) return;

         const clipItem = focusedElement.closest('.clip-review-item');
         if (!clipItem) return;
         const clipId = clipItem.dataset.clipId;
         const viewerState = spriteViewers[clipId];
         if (!viewerState || !viewerState.meta || !viewerState.meta.isValid) return; // Need valid state

         // Spacebar: Toggle Play/Pause
         if (event.code === 'Space') {
             event.preventDefault();
             togglePlayback(clipId);
             return;
         }

         let frameChange = 0;
         if (event.key === 'ArrowLeft') frameChange = -1;
         else if (event.key === 'ArrowRight') frameChange = 1;

         // Arrow Keys: Scrubbing
         if (frameChange !== 0) {
             event.preventDefault();
             if (viewerState.isPlaying) togglePlayback(clipId);
             const newFrame = viewerState.currentFrame + frameChange;
             updateSpriteFrame(clipId, newFrame);
             // updateSplitControls(clipId, newFrame); // Already called within updateSpriteFrame if needed
             return;
         }

         // Enter/Escape: Only in Split Mode
         if (viewerState.isSplitMode) {
              const confirmSplitBtn = clipItem.querySelector('.confirm-split-btn');
              const cancelSplitBtn = clipItem.querySelector('.cancel-split-btn');
              if (event.key === 'Enter' && confirmSplitBtn && !confirmSplitBtn.disabled) {
                  event.preventDefault();
                  console.log("Enter pressed in split mode - clicking confirm");
                  confirmSplitBtn.click();
              } else if (event.key === 'Escape' && cancelSplitBtn) {
                   event.preventDefault();
                   console.log("Escape pressed in split mode - clicking cancel");
                   cancelSplitBtn.click();
              }
         }

     }); // End of keydown listener

}); // End of DOMContentLoaded
// --- END OF FILE static/review.js ---