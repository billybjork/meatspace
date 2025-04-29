export const SpritePlayerController = {
    mounted() {
      const clipId = this.el.dataset.clipId;
      const playerData = this.el.dataset.player;
  
      if (!playerData) {
        console.warn("[SpritePlayer] data-player attribute missing on element", this.el);
        return;
      }
  
      let meta;
      try {
        meta = JSON.parse(playerData);
      } catch (err) {
        console.error("[SpritePlayer] Failed to parse JSON in data-player", playerData, err);
        return;
      }
  
      if (!meta.isValid) {
        console.error("[SpritePlayer] Invalid meta", meta);
        return;
      }
  
      const container = this.el.parentElement;
      const scrub = container.querySelector(`#scrub-${clipId}`);
      const playPause = container.querySelector(`#playpause-${clipId}`);
      const frameLabel = container.querySelector(`#frame-display-${clipId}`);
  
      if (!scrub || !playPause || !frameLabel) {
        console.warn("[SpritePlayer] Controls missing for clip", clipId);
        return;
      }
  
      this.player = new SpritePlayer(
        clipId, this.el, scrub, playPause, frameLabel, meta, null
      );
  
      // auto-play on mount
      this.player.play("mounted");
    },
  
    updated() {
      if (this.player) {
        this.player.cleanup();
      }
      this.mounted(); // re-mount on patch
    },
  
    destroyed() {
      if (this.player) {
        this.player.cleanup();
      }
    }
  };   

class SpritePlayer {
    constructor(clipId, viewerElement, scrubElement, playPauseBtn, frameDisplayElement, meta, updateSplitUICallback) { // Added callback parameter
        this.clipId = clipId;
        this.viewerElement = viewerElement;
        this.scrubElement = scrubElement;
        this.playPauseBtn = playPauseBtn;
        this.frameDisplayElement = frameDisplayElement;
        this.meta = meta; // Assumes meta is already validated and parsed
        this.updateSplitUICallback = updateSplitUICallback; // Store the callback

        this.currentFrame = 0;
        this.isPlaying = false;
        this.playbackInterval = null;
        this.isScrubbing = false; // Track if user is actively scrubbing
        this.isFocusedForKeys = false; // Tracks if viewer has focus for keyboard events (set by review-main.js)
        this.isToggling = false; // Prevent immediate re-toggling of play/pause

        this._setupUI();
        this._attachEventListeners();
    }

    _setupUI() {
        if (!this.viewerElement) {
            console.error(`[Player ${this.clipId}] Setup failed: viewerElement is null.`);
            return; // Cannot proceed without viewer
        }
        if (!this.meta || !this.meta.isValid) {
            this.viewerElement.textContent = 'Error: Invalid metadata provided.';
            this.viewerElement.classList.add('no-sprite');
            if (this.scrubElement) this.scrubElement.disabled = true;
            if (this.playPauseBtn) this.playPauseBtn.disabled = true;
            console.error(`[Player ${this.clipId}] Setup aborted due to invalid metadata.`);
            return;
        }

        // Set background image and size
        this.viewerElement.style.backgroundImage = `url('${this.meta.spriteUrl}')`;
        const bgWidth = this.meta.cols * this.meta.tile_width;
        const bgHeight = this.meta.rows * this.meta.tile_height_calculated;
        this.viewerElement.style.backgroundSize = `${bgWidth}px ${bgHeight}px`;

        // Set viewer dimensions based on tile size
        this.viewerElement.style.width = `${this.meta.tile_width}px`;
        this.viewerElement.style.height = `${this.meta.tile_height_calculated}px`;

        this.viewerElement.textContent = ''; // Clear any loading/error message
        this.viewerElement.classList.remove('no-sprite');

        // Setup scrub bar
        if (this.scrubElement) {
            // Max frame index is total_frames - 1
            this.scrubElement.max = this.meta.clip_total_frames > 0 ? this.meta.clip_total_frames - 1 : 0;
            this.scrubElement.value = 0; // Start at frame 0
            this.scrubElement.disabled = false;
        }
         // Setup play/pause button
        if (this.playPauseBtn) {
            this.playPauseBtn.disabled = false;
            this.playPauseBtn.textContent = '⏯️'; // Reset symbol (play/pause icon)
        }

        // Set the initial frame display and position
        this.updateFrame(0, true); // Initialize to frame 0 and force UI update
        // console.log(`[Player ${this.clipId}] UI setup complete. Dimensions: ${this.meta.tile_width}x${this.meta.tile_height_calculated}, Frames: ${this.meta.clip_total_frames}`);
    }

    _attachEventListeners() {
        // Play/Pause Button Click
        if (this.playPauseBtn) {
            // Use a named function property for easy removal during cleanup
            this._handlePlayPauseClick = (event) => {
                event.stopPropagation(); // Prevent click bubbling up (e.g., to clipItem listener)
                if (!this.playPauseBtn.disabled) { // Check if button is enabled
                    // console.log(`[Player ${this.clipId}] Play/Pause BUTTON CLICKED`);
                    this.togglePlayback();
                }
            };
            this.playPauseBtn.addEventListener('click', this._handlePlayPauseClick);
        }

        // Scrub Bar Interaction
        if (this.scrubElement) {
             // Named functions for event listeners
             this._handleScrubStart = () => {
                 // console.log(`[Player ${this.clipId}] Scrub Start (mousedown/touchstart)`);
                 this.isScrubbing = true;
                 if (this.isPlaying) {
                     this.pause("scrubStart"); // Pause playback while scrubbing
                 }
             };
             this._handleScrubEnd = () => {
                 // console.log(`[Player ${this.clipId}] Scrub End (mouseup/touchend)`);
                 this.isScrubbing = false;
             };
             this._handleScrubInput = (e) => {
                 const frame = parseInt(e.target.value, 10);
                 // Update frame immediately on input event (more responsive than 'change')
                 this.updateFrame(frame, true); // Force UI update to keep split UI sync'd
             };

            // Use 'input' for real-time update as the slider moves
            this.scrubElement.addEventListener('input', this._handleScrubInput);
            // Use mousedown/touchstart to detect start of scrubbing
            this.scrubElement.addEventListener('mousedown', this._handleScrubStart);
            this.scrubElement.addEventListener('touchstart', this._handleScrubStart, { passive: true }); // Optimize touch scrolling
            // Use mouseup/touchend to detect end of scrubbing
            this.scrubElement.addEventListener('mouseup', this._handleScrubEnd);
            this.scrubElement.addEventListener('touchend', this._handleScrubEnd);
        }
        // Focus/Blur listeners (related to 'isFocusedForKeys') are attached in review-main.js
        // Keydown listener is attached globally or to viewer in review-main.js
    }

    /**
     * Calculates the background X/Y position for the sprite sheet based on the frame number.
     * Maps the clip frame number to the corresponding frame in the sprite sheet.
     * @param {number} frameNumber - The desired frame number of the *clip segment* (0 to clip_total_frames - 1).
     * @returns {{bgX: number, bgY: number}} - The calculated background-position X and Y offsets.
     */
    _calculateBGPosition(frameNumber) {
         // Ensure metadata is valid before calculating
         if (!this.meta || !this.meta.isValid) return { bgX: 0, bgY: 0 };

         // Clamp frame number to valid range (0 to N-1)
         const maxClipFrame = this.meta.clip_total_frames > 0 ? this.meta.clip_total_frames - 1 : 0;
         frameNumber = Math.max(0, Math.min(frameNumber, maxClipFrame));

         // --- Map Clip Frame to Sprite Frame ---
         // Determine the proportion of the way through the *clip segment*
         const proportionThroughClip = (this.meta.clip_total_frames > 1) ? (frameNumber / maxClipFrame) : 0;
         // Calculate the corresponding target frame index within the *sprite sheet*
         const targetSpriteFrame = proportionThroughClip * (this.meta.total_sprite_frames - 1);
         // Round to the nearest whole sprite frame index and clamp to valid sprite frame range
         const spriteFrameIndex = Math.max(0, Math.min(Math.round(targetSpriteFrame), this.meta.total_sprite_frames - 1));

         // Calculate the row and column in the sprite grid
         const col = spriteFrameIndex % this.meta.cols;
         const row = Math.floor(spriteFrameIndex / this.meta.cols);

         // Get tile dimensions
         const tileW = this.meta.tile_width;
         const tileH = this.meta.tile_height_calculated;

         // Basic check for valid tile dimensions
         if (isNaN(tileW) || isNaN(tileH) || tileW <= 0 || tileH <= 0) {
              console.error(`[Player ${this.clipId} _calculateBGPosition] Invalid tile dimensions W=${tileW}, H=${tileH}`);
              return { bgX: 0, bgY: 0 }; // Return default position on error
         }

         // Calculate the negative offset for background-position
         const bgX = -(col * tileW);
         const bgY = -(row * tileH);

         return { bgX, bgY };
    }

    /**
     * Updates the player to display a specific frame.
     * @param {number} frameNumber - The target frame number (0-based).
     * @param {boolean} [forceUIUpdate=false] - If true, forces updates to scrub bar and text display even if frame number hasn't changed. Useful for sync after scrubbing or on init.
     */
    updateFrame(frameNumber, forceUIUpdate = false) {
        if (!this.meta || !this.meta.isValid) {
             // console.warn(`[Player ${this.clipId} updateFrame] Aborted: Invalid meta.`);
             return; // Cannot update if metadata is invalid
        }

        const maxFrame = this.meta.clip_total_frames > 0 ? this.meta.clip_total_frames - 1 : 0;
        // Ensure frame number is an integer within the valid range [0, maxFrame]
        frameNumber = Math.round(Math.max(0, Math.min(frameNumber, maxFrame)));

        // Avoid unnecessary updates if the frame hasn't changed, unless forced
        if (frameNumber === this.currentFrame && !forceUIUpdate) {
            return;
        }

        // console.debug(`[Player ${this.clipId} updateFrame] Updating to frame ${frameNumber}`); // Can be noisy
        this.currentFrame = frameNumber; // Update internal frame state

        try {
            // Update background position
            const { bgX, bgY } = this._calculateBGPosition(frameNumber);
            if (this.viewerElement) { // Check element exists before styling
                this.viewerElement.style.backgroundPosition = `${bgX}px ${bgY}px`;
            }

            // Update text display
            if (this.frameDisplayElement) {
                this.frameDisplayElement.textContent = `Frame: ${frameNumber}`;
            }

            // Update scrub bar position *only if* the user is not actively dragging it
            if (this.scrubElement && !this.isScrubbing) {
                this.scrubElement.value = frameNumber;
            }

            // --- Callback for Split UI ---
            // If a callback function was provided during construction, call it
            // This allows review-main.js to update the split controls panel
            if (typeof this.updateSplitUICallback === 'function') {
                 // Pass the clipId (redundant but consistent), current frame, and metadata
                 this.updateSplitUICallback(this.clipId, this.currentFrame, this.meta);
            }

        } catch (error) {
            console.error(`[Player ${this.clipId} updateFrame] Error during UI update for frame ${frameNumber}:`, error);
            this.pause("updateFrameError"); // Stop playback if UI update fails catastrophically
        }
    }

    /** Toggles the playback state between playing and paused. */
    togglePlayback() {
        // Debounce rapid toggles using a flag and timeout
        if (this.isToggling) {
            // console.log(`[Player ${this.clipId}] Toggle ignored - already toggling (debounce).`);
            return;
        }
        this.isToggling = true;

        // Check if player is in a valid state to toggle
        if (!this.meta || !this.meta.isValid) {
            console.warn(`[Player ${this.clipId}] Cannot toggle playback: Invalid meta or setup failed.`);
            this.isToggling = false; // Reset flag immediately if invalid
            return;
        }

        // console.log(`[Player ${this.clipId}] togglePlayback called. Current state isPlaying: ${this.isPlaying}`);

        // Call play or pause based on current state
        if (this.isPlaying) {
            this.pause("togglePlayback");
        } else {
            this.play("togglePlayback");
        }

        // Reset the toggle flag after a short delay to allow state change
        setTimeout(() => {
             this.isToggling = false;
        }, 50); // 50ms debounce window
    }

    /** Starts playback using setInterval. */
    play(source = "unknown") {
        // Prevent starting if already playing, metadata invalid, or FPS is zero/negative
        if (this.isPlaying || !this.meta || !this.meta.isValid || this.meta.clip_fps <= 0) {
             console.log(`[Player ${this.clipId}] play() called from '${source}' but ignored (isPlaying=${this.isPlaying}, metaValid=${this.meta?.isValid}, fps=${this.meta?.clip_fps})`);
             return;
        }

        this.isPlaying = true;
        if (this.playPauseBtn) this.playPauseBtn.textContent = '⏸️'; // Update button to Pause symbol

        // Calculate interval time based on clip FPS
        const intervalTime = 1000 / this.meta.clip_fps;
        // console.log(`[Player ${this.clipId}] Playback STARTING (called from '${source}', Interval: ${intervalTime.toFixed(2)}ms, FPS: ${this.meta.clip_fps})`);

        // Clear any potentially orphaned interval before starting a new one
        if (this.playbackInterval) {
             console.warn(`[Player ${this.clipId}] Cleared existing interval before starting new one.`);
             clearInterval(this.playbackInterval);
             this.playbackInterval = null; // Explicitly nullify
        }

        // --- Set up the playback interval ---
        this.playbackInterval = setInterval(() => {
            // Use try-catch inside interval to prevent it from stopping silently on error
            try {
                // Check if this specific instance should still be playing and has valid metadata.
                if (!this.isPlaying || !this.meta?.isValid) {
                     console.log(`[Player ${this.clipId}] Interval stopping (Internal Check: isPlaying=${this.isPlaying}, metaValid=${this.meta?.isValid})`);
                     this.pause("intervalCheck"); // Call pause on 'this' player instance
                     return; // Stop this interval iteration
                }

                // Calculate the next frame number
                let nextFrame = this.currentFrame + 1;
                // Loop back to frame 0 if we reach the end
                if (nextFrame >= this.meta.clip_total_frames) {
                    nextFrame = 0;
                }
                // Update the frame display
                this.updateFrame(nextFrame, true); // Force UI update during playback

            } catch (error) {
                console.error(`[Player ${this.clipId}] Error inside playback interval:`, error);
                this.pause("intervalError"); // Stop playback immediately on error
            }
        }, intervalTime);
    }

    /** Pauses playback by clearing the interval. */
    pause(source = "unknown") {
        // Log only if it was actually playing to reduce noise
        if (this.isPlaying) {
            // console.log(`[Player ${this.clipId}] Playback PAUSING (called from '${source}')`);
        }

        // Clear the interval if it exists
        if (this.playbackInterval) {
            clearInterval(this.playbackInterval);
            this.playbackInterval = null; // Important to nullify after clearing
            // console.debug(`[Player ${this.clipId}] Interval cleared.`);
        } else if (this.isPlaying) {
            // Log if we were in playing state but interval was already null (shouldn't happen ideally)
             console.warn(`[Player ${this.clipId}] Was in playing state but interval was already null during pause call.`);
        }

        this.isPlaying = false; // Update state regardless of interval existence
        // Update button to Play symbol (or Play/Pause composite)
        if (this.playPauseBtn) {
            this.playPauseBtn.textContent = '⏯️';
        }
    }

    /** Public method to get the current frame number. */
    getCurrentFrame() {
        return this.currentFrame;
    }

    /**
     * Cleans up resources used by the player instance.
     * Should be called before the player/clip item is removed from the DOM or page reloads.
    */
    cleanup() {
        console.log(`[Player ${this.clipId}] Starting cleanup...`);
        this.pause("cleanup"); // Ensure interval is cleared and state is set to not playing

        // Remove specific event listeners added by this instance
        if (this.playPauseBtn && this._handlePlayPauseClick) {
            this.playPauseBtn.removeEventListener('click', this._handlePlayPauseClick);
            this._handlePlayPauseClick = null; // Remove reference to the handler function
        }
        if (this.scrubElement) {
            if (this._handleScrubInput) this.scrubElement.removeEventListener('input', this._handleScrubInput);
            if (this._handleScrubStart) this.scrubElement.removeEventListener('mousedown', this._handleScrubStart);
            if (this._handleScrubStart) this.scrubElement.removeEventListener('touchstart', this._handleScrubStart);
            if (this._handleScrubEnd) this.scrubElement.removeEventListener('mouseup', this._handleScrubEnd);
            if (this._handleScrubEnd) this.scrubElement.removeEventListener('touchend', this._handleScrubEnd);
             // Nullify handler references
             this._handleScrubInput = null;
             this._handleScrubStart = null;
             this._handleScrubEnd = null;
        }

        // Nullify references to DOM elements to help garbage collection
        this.viewerElement = null;
        this.scrubElement = null;
        this.playPauseBtn = null;
        this.frameDisplayElement = null;
        this.meta = null; // Release metadata object reference
        this.updateSplitUICallback = null; // Release callback reference

        console.log(`[Player ${this.clipId}] Cleanup finished.`);
        // Note: Global listeners (keydown/keyup/beforeunload) are managed in intake-review.js
    }
}