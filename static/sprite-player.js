class SpritePlayer {
    constructor(clipId, viewerElement, scrubElement, playPauseBtn, frameDisplayElement, meta) {
        this.clipId = clipId;
        this.viewerElement = viewerElement;
        this.scrubElement = scrubElement;
        this.playPauseBtn = playPauseBtn;
        this.frameDisplayElement = frameDisplayElement;
        this.meta = meta; // Assumes meta is already validated and parsed

        this.currentFrame = 0;
        this.isPlaying = false;
        this.playbackInterval = null;
        this.isScrubbing = false; // Track if user is actively scrubbing
        this.isFocusedForKeys = false;
        this.isToggling = false; // Prevent immediate re-toggling

        this._setupUI();
        this._attachEventListeners();
    }

    _setupUI() {
        if (!this.meta || !this.meta.isValid) {
            this.viewerElement.textContent = 'Error: Invalid metadata provided.';
            this.viewerElement.classList.add('no-sprite');
            if (this.scrubElement) this.scrubElement.disabled = true;
            if (this.playPauseBtn) this.playPauseBtn.disabled = true;
            console.error(`[Player ${this.clipId}] Setup aborted due to invalid metadata.`);
            return;
        }

        // Set background image and size
        this.viewerElement.style.backgroundImage = `url('${this.meta.spriteUrl}')`; // Assuming spriteUrl is added to meta during init
        const bgWidth = this.meta.cols * this.meta.tile_width;
        const bgHeight = this.meta.rows * this.meta.tile_height_calculated;
        this.viewerElement.style.backgroundSize = `${bgWidth}px ${bgHeight}px`;

        // Set viewer dimensions
        this.viewerElement.style.width = `${this.meta.tile_width}px`;
        this.viewerElement.style.height = `${this.meta.tile_height_calculated}px`;

        this.viewerElement.textContent = ''; // Clear any loading message
        this.viewerElement.classList.remove('no-sprite'); // Ensure error class is removed

        // Setup scrub bar
        if (this.scrubElement) {
            this.scrubElement.max = this.meta.clip_total_frames > 0 ? this.meta.clip_total_frames - 1 : 0;
            this.scrubElement.disabled = false;
        }
         // Setup play/pause button
        if (this.playPauseBtn) {
            this.playPauseBtn.disabled = false;
            this.playPauseBtn.textContent = '⏯️'; // Reset symbol
        }

        this.updateFrame(0, true); // Initialize to frame 0 and update UI
        console.log(`[Player ${this.clipId}] UI setup complete.`);
    }

    _attachEventListeners() {
        if (this.playPauseBtn) {
            // Use named function for easier removal if needed later
            this._handlePlayPauseClick = (event) => {
                // Explicitly stop event propagation to prevent potential bubbling issues
                event.stopPropagation();
                console.log(`[Player ${this.clipId}] Play/Pause BUTTON CLICKED`);
                this.togglePlayback();
            };
            this.playPauseBtn.addEventListener('click', this._handlePlayPauseClick);
        }
        if (this.scrubElement) {
             // Use named functions for listeners
             this._handleScrubStart = () => {
                 console.log(`[Player ${this.clipId}] Scrub Start (mousedown/touchstart)`);
                 this.isScrubbing = true;
                 if(this.isPlaying) this.pause("scrubStart");
             };
             this._handleScrubEnd = () => {
                 console.log(`[Player ${this.clipId}] Scrub End (mouseup/touchend)`);
                 this.isScrubbing = false;
             };
             this._handleScrubInput = (e) => {
                 const frame = parseInt(e.target.value, 10);
                 // console.log(`[Player ${this.clipId}] Scrub Input: frame ${frame}`); // Can be noisy
                 this.updateFrame(frame);
             };

            this.scrubElement.addEventListener('mousedown', this._handleScrubStart);
            this.scrubElement.addEventListener('touchstart', this._handleScrubStart, { passive: true }); // Use passive for touchstart scroll performance
            this.scrubElement.addEventListener('mouseup', this._handleScrubEnd);
            this.scrubElement.addEventListener('touchend', this._handleScrubEnd);
            this.scrubElement.addEventListener('input', this._handleScrubInput);
        }
        // Note: Focus/Blur listeners that set isFocusedForKeys are attached in review-main.js
    }

    // Calculates background position for a given frame
    _calculateBGPosition(frameNumber) {
         if (!this.meta || !this.meta.isValid) return { bgX: 0, bgY: 0 };

         frameNumber = Math.max(0, Math.min(frameNumber, this.meta.clip_total_frames - 1));
         const proportionThroughClip = (this.meta.clip_total_frames > 1) ? (frameNumber / (this.meta.clip_total_frames - 1)) : 0;
         const targetSpriteFrame = proportionThroughClip * (this.meta.total_sprite_frames - 1);
         // Use Math.round for potentially smoother mapping when sprite FPS is high
         const spriteFrameIndex = Math.max(0, Math.min(Math.round(targetSpriteFrame), this.meta.total_sprite_frames - 1));

         const col = spriteFrameIndex % this.meta.cols;
         const row = Math.floor(spriteFrameIndex / this.meta.cols);

         const tileW = this.meta.tile_width;
         const tileH = this.meta.tile_height_calculated;

         if (tileW <=0 || tileH <= 0) {
              console.error(`[Player ${this.clipId} _calculateBGPosition] Invalid tile dimensions W=${tileW}, H=${tileH}`);
              return { bgX: 0, bgY: 0 };
         }

         const bgX = -(col * tileW);
         const bgY = -(row * tileH);
         return { bgX, bgY };
    }

    // Public method to update the viewer to a specific frame
    // forceUIUpdate flag ensures elements like scrub bar/text display are updated even if frame hasn't changed numerically (useful on init)
    updateFrame(frameNumber, forceUIUpdate = false) {
        if (!this.meta || !this.meta.isValid) {
             // console.warn(`[Player ${this.clipId} updateFrame] Aborted: Invalid meta.`);
             return;
        }

        const maxFrame = this.meta.clip_total_frames > 0 ? this.meta.clip_total_frames - 1 : 0;
        frameNumber = Math.max(0, Math.min(frameNumber, maxFrame));
        frameNumber = Math.round(frameNumber);

        // Only update visuals if frame actually changed or forced
        if (frameNumber === this.currentFrame && !forceUIUpdate) {
            // console.debug(`[Player ${this.clipId} updateFrame] Frame ${frameNumber} same as current, skipping UI update.`);
            return;
        }

        // console.debug(`[Player ${this.clipId} updateFrame] Updating to frame ${frameNumber}`);
        this.currentFrame = frameNumber;

        try {
            const { bgX, bgY } = this._calculateBGPosition(frameNumber);
            if (this.viewerElement) { // Check if element still exists
                this.viewerElement.style.backgroundPosition = `${bgX}px ${bgY}px`;
            }

            if (this.frameDisplayElement) {
                this.frameDisplayElement.textContent = `Frame: ${frameNumber}`;
            }
            // Update scrub bar only if user isn't actively dragging it
            if (this.scrubElement && !this.isScrubbing) {
                this.scrubElement.value = frameNumber;
            }
        } catch (error) {
            console.error(`[Player ${this.clipId} updateFrame] Error during UI update for frame ${frameNumber}:`, error);
            this.pause("updateFrameError"); // Stop playback if UI update fails
        }
    }

    // Public method to toggle playback
    togglePlayback() {
        // Use a short timeout to prevent double-toggling from rapid events
        if (this.isToggling) {
            console.warn(`[Player ${this.clipId}] Toggle ignored - already toggling.`);
            return;
        }
        this.isToggling = true;

        if (!this.meta || !this.meta.isValid) {
            console.warn(`[Player ${this.clipId}] Cannot toggle playback: Invalid meta.`);
            this.isToggling = false;
            return;
        }

        console.log(`[Player ${this.clipId}] togglePlayback called. Current state isPlaying: ${this.isPlaying}`);

        if (this.isPlaying) {
            this.pause("togglePlayback"); // Pass source for logging
        } else {
            this.play("togglePlayback"); // Pass source for logging
        }

        // Reset flag after a short delay
        setTimeout(() => {
             // console.debug(`[Player ${this.clipId}] Resetting toggle flag.`);
             this.isToggling = false;
        }, 50); // Reduced delay
    }

    play(source = "unknown") {
        if (this.isPlaying || !this.meta || !this.meta.isValid || this.meta.clip_fps <= 0) {
             console.log(`[Player ${this.clipId}] play() called from '${source}' but ignored (isPlaying=${this.isPlaying}, metaValid=${this.meta?.isValid}, fps=${this.meta?.clip_fps})`);
             return; // Exit if already playing or invalid state
        }

        this.isPlaying = true;
        if (this.playPauseBtn) this.playPauseBtn.textContent = '⏸️';
        const intervalTime = 1000 / this.meta.clip_fps;
        console.log(`[Player ${this.clipId}] Playback STARTING (called from '${source}', Interval: ${intervalTime.toFixed(2)}ms, FPS: ${this.meta.clip_fps})`);

        // Clear any existing interval just in case (e.g., if pause didn't clear properly)
        if(this.playbackInterval) {
             console.warn(`[Player ${this.clipId}] Cleared existing interval before starting new one.`);
             clearInterval(this.playbackInterval);
        }

        this.playbackInterval = setInterval(() => {
            // Use try-catch inside interval to prevent it from stopping silently on error
            try {
                // Check global state map existence for safety - maybe the player was cleaned up
                const globalPlayerInstance = window.spritePlayers && window.spritePlayers[this.clipId]; // Assume players are stored globally

                if (!this.isPlaying || !globalPlayerInstance || !globalPlayerInstance.meta?.isValid) {
                     console.log(`[Player ${this.clipId}] Interval stopping (isPlaying=${this.isPlaying}, instanceExists=${!!globalPlayerInstance}, metaValid=${globalPlayerInstance?.meta?.isValid})`);
                     this.pause("intervalCheck"); // Call pause to ensure cleanup and button reset
                     return;
                }

                let nextFrame = this.currentFrame + 1;
                if (nextFrame >= this.meta.clip_total_frames) {
                    nextFrame = 0; // Loop
                }
                this.updateFrame(nextFrame, true); // Force UI update during playback
            } catch (error) {
                console.error(`[Player ${this.clipId}] Error inside playback interval:`, error);
                this.pause("intervalError"); // Stop playback on error
            }
        }, intervalTime);
    }

    pause(source = "unknown") {
        // Only log pause if it was actually playing
        if (this.isPlaying) {
            console.log(`[Player ${this.clipId}] Playback PAUSING (called from '${source}')`);
        } else {
            // console.debug(`[Player ${this.clipId}] pause() called from '${source}' but was already paused.`);
        }

        if(this.playbackInterval) {
            clearInterval(this.playbackInterval);
            this.playbackInterval = null;
            // console.debug(`[Player ${this.clipId}] Interval cleared.`);
        } else if (this.isPlaying){
            // Log if we are in playing state but interval is somehow null
             console.warn(`[Player ${this.clipId}] Was in playing state but interval was already null during pause call.`);
        }

        this.isPlaying = false;
        if (this.playPauseBtn) {
            this.playPauseBtn.textContent = '⏯️';
        }
    }

    // Public method to get current frame
    getCurrentFrame() {
        return this.currentFrame;
    }

    // Cleanup method to be called when the player is no longer needed
    cleanup() {
        console.log(`[Player ${this.clipId}] Starting cleanup...`);
        this.pause("cleanup"); // Ensure interval is cleared

        // Remove specific listeners added by this instance
        if (this.playPauseBtn && this._handlePlayPauseClick) {
            this.playPauseBtn.removeEventListener('click', this._handlePlayPauseClick);
        }
        if (this.scrubElement) {
            if (this._handleScrubStart) this.scrubElement.removeEventListener('mousedown', this._handleScrubStart);
            if (this._handleScrubStart) this.scrubElement.removeEventListener('touchstart', this._handleScrubStart);
            if (this._handleScrubEnd) this.scrubElement.removeEventListener('mouseup', this._handleScrubEnd);
            if (this._handleScrubEnd) this.scrubElement.removeEventListener('touchend', this._handleScrubEnd);
            if (this._handleScrubInput) this.scrubElement.removeEventListener('input', this._handleScrubInput);
        }

        // Nullify references to DOM elements
        this.viewerElement = null;
        this.scrubElement = null;
        this.playPauseBtn = null;
        this.frameDisplayElement = null;
        this.meta = null; // Release metadata object

        console.log(`[Player ${this.clipId}] Cleanup finished.`);
    }
}