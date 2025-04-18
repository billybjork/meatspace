document.addEventListener('DOMContentLoaded', () => {
    const reviewQueue = document.getElementById('review-queue');
    const players = {}; // Store SpritePlayer instances { clipId: playerInstance }
    const splitModeState = {}; // Store split mode UI state { clipId: { isActive: boolean } }

    // --- Initialization ---
    document.querySelectorAll('.clip-review-item').forEach(clipItem => {
        const clipId = clipItem.dataset.clipId;
        const spriteUrl = clipItem.dataset.spriteUrl;
        const spriteMetaJson = clipItem.dataset.spriteMeta;
        const viewer = document.getElementById(`sprite-viewer-${clipId}`);
        const scrubBar = document.getElementById(`scrub-${clipId}`);
        const playPauseBtn = clipItem.querySelector('.sprite-play-pause-btn');
        const frameDisplay = document.getElementById(`frame-display-${clipId}`);
        const controlsDiv = document.getElementById(`controls-${clipId}`);
        const splitBtn = clipItem.querySelector('.split-mode-btn');

        splitModeState[clipId] = { isActive: false }; // Initialize split state
        let meta = null;
        let initError = null;

        if (viewer) viewer.textContent = 'Initializing...';

        if (spriteUrl && spriteMetaJson && viewer) {
            try {
                // Double Parse Logic
                let parsedOnce = JSON.parse(spriteMetaJson);
                meta = (typeof parsedOnce === 'string') ? JSON.parse(parsedOnce) : parsedOnce;

                // Validate parsed object
                if (!meta || typeof meta !== 'object') throw new Error("Metadata did not resolve to a valid object.");

                const parsedMeta = { // Convert properties and validate
                    tile_width: parseFloat(meta.tile_width),
                    tile_height_calculated: parseFloat(meta.tile_height_calculated),
                    cols: parseInt(meta.cols, 10),
                    rows: parseInt(meta.rows, 10),
                    total_sprite_frames: parseInt(meta.total_sprite_frames, 10),
                    clip_fps: parseFloat(meta.clip_fps),
                    clip_total_frames: parseInt(meta.clip_total_frames, 10),
                    spriteUrl: spriteUrl // Add URL for convenience
                };

                // Simplified validation checks
                const requiredKeys = ['tile_width', 'tile_height_calculated', 'cols', 'rows', 'total_sprite_frames', 'clip_fps', 'clip_total_frames'];
                for (const key of requiredKeys) {
                     if (isNaN(parsedMeta[key]) || parsedMeta[key] <= 0) {
                        throw new Error(`Invalid or missing numeric value for '${key}': ${meta[key]}`);
                     }
                }
                parsedMeta.isValid = true;

                // Create and store the player instance
                players[clipId] = new SpritePlayer(clipId, viewer, scrubBar, playPauseBtn, frameDisplay, parsedMeta);
                console.log(`[Main Init ${clipId}] SpritePlayer initialized successfully.`);

            } catch (e) {
                console.error(`[Main Init ${clipId}] Failed during initialization:`, e);
                initError = `Error initializing sprite: ${e.message}`;
                if(viewer) { viewer.textContent = initError; viewer.classList.add('no-sprite'); }
            }
        } else {
             initError = 'Sprite sheet URL or metadata attribute missing.';
             if(viewer) { viewer.textContent = initError; viewer.classList.add('no-sprite'); }
        }

        // Final error handling / disabling controls
        if (initError) {
             console.error(`[Main Init ${clipId}] Disabling controls due to error: ${initError}`);
             if (scrubBar) scrubBar.disabled = true;
             if (playPauseBtn) playPauseBtn.disabled = true;
             if (controlsDiv) controlsDiv.style.opacity = '0.5';
             if (splitBtn) splitBtn.disabled = true;
        } else if (!spriteUrl && viewer) { // Handle legitimately missing sprites
            const spriteErrorMsg = clipItem.querySelector('.clip-info strong[style*="color:orange"]');
            viewer.textContent = spriteErrorMsg ? 'Sprite generation failed.' : 'Sprite sheet not generated.';
            viewer.classList.add('no-sprite');
            if (scrubBar) scrubBar.disabled = true;
            if (playPauseBtn) playPauseBtn.disabled = true;
            if (controlsDiv) controlsDiv.style.opacity = '0.5';
            if (splitBtn) splitBtn.disabled = true;
        }

        // Add focus listeners here, interacting with the player instance if it exists
         if (viewer) {
             viewer.setAttribute('tabindex', '0'); // Ensure focusable even if init failed (for potential retry?)
             viewer.addEventListener('focus', () => {
                  const player = players[clipId];
                  if (player) player.isFocusedForKeys = true; // Let player know it's focused
                  viewer.style.outline = '2px solid dodgerblue';
             });
             viewer.addEventListener('blur', () => {
                  const player = players[clipId];
                  if (player) player.isFocusedForKeys = false;
                  viewer.style.outline = 'none';
             });
         }

    }); // End of initialization loop


    // --- Split Mode UI Management ---
    function activateSplitMode(clipId, clipItem, player) {
        if (!player || !player.meta?.isValid) {
            alert("Cannot enter split mode: Sprite data invalid or player not initialized.");
            return;
        }
        splitModeState[clipId].isActive = true;
        player.pause(); // Ensure paused

        const splitControls = clipItem.querySelector('.split-controls');
        const splitModeBtn = clipItem.querySelector('.split-mode-btn');
        const splitFeedback = clipItem.querySelector('.split-feedback');
        const viewer = document.getElementById(`sprite-viewer-${clipId}`);

        if (splitControls) splitControls.style.display = 'block';
        if (splitModeBtn) splitModeBtn.style.display = 'none';
        if (splitFeedback) splitFeedback.textContent = '';

        // Update UI with current frame
        player.updateFrame(player.getCurrentFrame(), true); // Force UI update
        updateSplitUI(clipId, player.getCurrentFrame(), player.meta);

        if (viewer) viewer.focus(); // Focus for keyboard input
        console.log(`[Main ${clipId}] Split mode activated.`);
    }

    function cancelSplitMode(clipId, clipItem) {
        splitModeState[clipId].isActive = false;
        const splitControls = clipItem.querySelector('.split-controls');
        const splitModeBtn = clipItem.querySelector('.split-mode-btn');
        const viewer = document.getElementById(`sprite-viewer-${clipId}`);

        if (splitControls) splitControls.style.display = 'none';
        if (splitModeBtn) splitModeBtn.style.display = 'inline-block';
        if (viewer) viewer.focus(); // Optional: refocus viewer
        console.log(`[Main ${clipId}] Split mode cancelled.`);
    }

    // Updates the static parts of the split UI (called by player updateFrame or activation)
    function updateSplitUI(clipId, frameNumber, meta) {
        const clipItem = document.getElementById(`clip-${clipId}`);
        if (!clipItem || !meta) return;

        const confirmFrameSpan = clipItem.querySelector('.split-confirm-frame');
        const confirmTimeSpan = clipItem.querySelector('.split-confirm-time');
        const confirmFrameBtnText = clipItem.querySelector('.split-confirm-frame-btn-text');
        const confirmSplitBtn = clipItem.querySelector('.confirm-split-btn');
        const splitFeedback = clipItem.querySelector('.split-feedback');
        const totalFramesSpan = clipItem.querySelector('.split-total-frames');

        if (!confirmFrameSpan || !confirmTimeSpan || !confirmFrameBtnText || !confirmSplitBtn || !splitFeedback || !totalFramesSpan) return;

        const totalFrames = meta.clip_total_frames;
        const fps = meta.clip_fps;
        confirmFrameSpan.textContent = frameNumber;
        confirmFrameBtnText.textContent = frameNumber;
        totalFramesSpan.textContent = totalFrames > 0 ? totalFrames - 1 : '?';
        const approxTime = (fps > 0 && totalFrames > 0) ? (frameNumber / fps) : 0;
        confirmTimeSpan.textContent = approxTime.toFixed(3);

        // Validation logic (copied from old updateSplitControls)
        const minFrameMargin = 1;
        let isDisabled = true;
        let feedbackMsg = '';
        if (totalFrames <= (2 * minFrameMargin)) { feedbackMsg = `Clip too short to split (Total Frames: ${totalFrames})`; }
        else if (frameNumber < minFrameMargin) { feedbackMsg = `Select frame ≥ ${minFrameMargin}.`; }
        else if (frameNumber >= (totalFrames - minFrameMargin)) { feedbackMsg = `Select frame ≤ ${totalFrames - minFrameMargin - 1}.`; }
        else { isDisabled = false; feedbackMsg = 'Valid split point.'; }

        confirmSplitBtn.disabled = isDisabled;
        splitFeedback.textContent = feedbackMsg;
        splitFeedback.style.color = isDisabled ? '#d00' : '#080';
    }


    // --- Main Event Listener for Actions ---
    reviewQueue.addEventListener('click', async (event) => {
        const target = event.target;
        const clipItem = target.closest('.clip-review-item');
        if (!clipItem) return;

        const clipId = clipItem.dataset.clipId;
        const player = players[clipId]; // Get player instance (might be undefined)
        const feedbackDiv = clipItem.querySelector('.action-feedback');
        const undoButton = clipItem.querySelector('.undo-button');
        const splitControls = clipItem.querySelector('.split-controls');


        // --- Player Controls ---
        if (target.matches('.sprite-play-pause-btn')) {
            if (player) player.togglePlayback();
            return;
        }

        // --- Split Mode ---
        if (target.matches('.split-mode-btn')) {
            activateSplitMode(clipId, clipItem, player);
            return;
        }
        if (target.matches('.cancel-split-btn')) {
            cancelSplitMode(clipId, clipItem);
            return;
        }
        if (target.matches('.confirm-split-btn')) {
            if (player) {
                player.pause(); // Ensure paused
                const frame = player.getCurrentFrame();
                const result = await queueSplit(clipId, frame, clipItem, feedbackDiv, splitControls);
                if (result.success) {
                     // Clean up player instance for completed item
                     if(players[clipId]) players[clipId].cleanup();
                     delete players[clipId];
                     delete splitModeState[clipId];
                }
            } else {
                 console.error("Cannot confirm split: Player instance not found for clip", clipId);
                 const splitFeedback = splitControls.querySelector('.split-feedback');
                 if(splitFeedback) splitFeedback.textContent = "Error: Player not initialized.";
            }
            return;
        }

        // --- Standard Actions & Undo ---
        if (target.matches('.action-btn')) { // Catches approve, skip, archive, merge, retry
            const action = target.dataset.action;
            if (player) player.pause(); // Pause playback before action
            const result = await handleAction(clipId, action, clipItem, feedbackDiv, undoButton);
            if (result.success && action !== 'skip' && action !== 'retry_sprite_gen') {
                // Clean up player instance if action implies completion
                if(players[clipId]) players[clipId].cleanup();
                delete players[clipId];
                delete splitModeState[clipId];
            }
            return;
        }
        if (target.matches('.undo-button')) {
             if (player) player.pause();
             const result = await handleUndo(clipId, clipItem, feedbackDiv, undoButton);
             if (result.success) {
                 // Clean up player instance on successful undo (will re-init on refresh)
                  if(players[clipId]) players[clipId].cleanup();
                  delete players[clipId];
                  delete splitModeState[clipId];
             }
            return;
        }

    }); // End click listener

    // --- Keyboard Listener ---
    document.addEventListener('keydown', (event) => {
         const focusedElement = document.activeElement;
         if (!focusedElement || !focusedElement.classList.contains('sprite-viewer')) return;

         const clipItem = focusedElement.closest('.clip-review-item');
         if (!clipItem) return;
         const clipId = clipItem.dataset.clipId;
         const player = players[clipId];
         const state = splitModeState[clipId];

         if (!player || !player.meta?.isValid) return; // Need valid player

         // Spacebar: Toggle Play/Pause
         if (event.code === 'Space') {
             event.preventDefault();
             player.togglePlayback();
             return;
         }

         let frameChange = 0;
         if (event.key === 'ArrowLeft') frameChange = -1;
         else if (event.key === 'ArrowRight') frameChange = 1;

         // Arrow Keys: Scrubbing
         if (frameChange !== 0) {
             event.preventDefault();
             player.pause(); // Pause on scrub
             const newFrame = player.getCurrentFrame() + frameChange;
             player.updateFrame(newFrame, true); // Force UI update
             // If in split mode, update the split UI as well
             if (state?.isActive) {
                  updateSplitUI(clipId, newFrame, player.meta);
             }
             return;
         }

         // Enter/Escape: Only in Split Mode
         if (state?.isActive) {
              const confirmSplitBtn = clipItem.querySelector('.confirm-split-btn');
              const cancelSplitBtn = clipItem.querySelector('.cancel-split-btn');
              if (event.key === 'Enter' && confirmSplitBtn && !confirmSplitBtn.disabled) {
                  event.preventDefault();
                  console.log("Enter pressed in split mode - clicking confirm");
                  confirmSplitBtn.click(); // Simulate click
              } else if (event.key === 'Escape' && cancelSplitBtn) {
                   event.preventDefault();
                   console.log("Escape pressed in split mode - clicking cancel");
                   cancelSplitBtn.click(); // Simulate click
              }
         }
     }); // End keydown listener

}); // End DOMContentLoaded