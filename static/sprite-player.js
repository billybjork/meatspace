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
        this.isScrubbing = false; // Flag to track if user is actively scrubbing

        this._setupUI();
        this._attachEventListeners();
    }

    _setupUI() {
        if (!this.meta || !this.meta.isValid) {
            this.viewerElement.textContent = 'Error: Invalid metadata provided.';
            this.viewerElement.classList.add('no-sprite');
            if (this.scrubElement) this.scrubElement.disabled = true;
            if (this.playPauseBtn) this.playPauseBtn.disabled = true;
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
        // this.viewerElement.style.aspectRatio = `${this.meta.tile_width} / ${this.meta.tile_height_calculated}`; // Optional

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
    }

    _attachEventListeners() {
        if (this.playPauseBtn) {
            this.playPauseBtn.addEventListener('click', () => this.togglePlayback());
        }
        if (this.scrubElement) {
             // Track scrubbing state
            this.scrubElement.addEventListener('mousedown', () => { this.isScrubbing = true; if(this.isPlaying) this.pause(); });
            this.scrubElement.addEventListener('touchstart', () => { this.isScrubbing = true; if(this.isPlaying) this.pause(); }); // For touch devices
            this.scrubElement.addEventListener('mouseup', () => { this.isScrubbing = false; });
            this.scrubElement.addEventListener('touchend', () => { this.isScrubbing = false; });

            this.scrubElement.addEventListener('input', (e) => {
                 const frame = parseInt(e.target.value, 10);
                 // Only update if scrubbing, otherwise playback interval might conflict
                 // if (this.isScrubbing) { // We paused on mousedown, so update should be fine
                     this.updateFrame(frame);
                 // }
            });
        }
        // Focus/Blur listeners remain in review-main.js as they relate to keyboard input scope
    }

    // Calculates background position for a given frame
    _calculateBGPosition(frameNumber) {
         if (!this.meta || !this.meta.isValid) return { bgX: 0, bgY: 0 };

         frameNumber = Math.max(0, Math.min(frameNumber, this.meta.clip_total_frames - 1));
         const proportionThroughClip = (this.meta.clip_total_frames > 1) ? (frameNumber / (this.meta.clip_total_frames - 1)) : 0;
         const targetSpriteFrame = proportionThroughClip * (this.meta.total_sprite_frames - 1);
         const spriteFrameIndex = Math.max(0, Math.min(Math.floor(targetSpriteFrame), this.meta.total_sprite_frames - 1));

         const col = spriteFrameIndex % this.meta.cols;
         const row = Math.floor(spriteFrameIndex / this.meta.cols);

         const tileW = this.meta.tile_width; // Already parsed number
         const tileH = this.meta.tile_height_calculated; // Already parsed number

         if (tileW <=0 || tileH <= 0) return { bgX: 0, bgY: 0 }; // Safety check

         const bgX = -(col * tileW);
         const bgY = -(row * tileH);
         return { bgX, bgY };
    }

    // Public method to update the viewer to a specific frame
    updateFrame(frameNumber, forceUIUpdate = false) {
        if (!this.meta || !this.meta.isValid) return;

        const maxFrame = this.meta.clip_total_frames > 0 ? this.meta.clip_total_frames - 1 : 0;
        frameNumber = Math.max(0, Math.min(frameNumber, maxFrame));
        frameNumber = Math.round(frameNumber);

        // Only update visuals if frame actually changed or forced
        if (frameNumber === this.currentFrame && !forceUIUpdate) return;

        this.currentFrame = frameNumber;

        const { bgX, bgY } = this._calculateBGPosition(frameNumber);
        this.viewerElement.style.backgroundPosition = `${bgX}px ${bgY}px`;

        if (this.frameDisplayElement) {
            this.frameDisplayElement.textContent = `Frame: ${frameNumber}`;
        }
        // Update scrub bar only if user isn't actively dragging it
        if (this.scrubElement && !this.isScrubbing) {
            this.scrubElement.value = frameNumber;
        }
    }

    // Public method to toggle playback
    togglePlayback() {
        if (!this.meta || !this.meta.isValid) {
            console.warn(`[Player ${this.clipId}] Cannot toggle playback: Invalid meta.`);
            return;
        }

        if (this.isPlaying) {
            this.pause();
        } else {
            this.play();
        }
    }

    play() {
        if (this.isPlaying || !this.meta || !this.meta.isValid || this.meta.clip_fps <= 0) return;

        this.isPlaying = true;
        if (this.playPauseBtn) this.playPauseBtn.textContent = '⏸️';
        const intervalTime = 1000 / this.meta.clip_fps;
        console.log(`[Player ${this.clipId}] Playback Started (Interval: ${intervalTime.toFixed(2)}ms, FPS: ${this.meta.clip_fps})`);

        this.playbackInterval = setInterval(() => {
            if (!this.isPlaying) { // Check if paused externally
                 clearInterval(this.playbackInterval);
                 return;
            }
            let nextFrame = this.currentFrame + 1;
            if (nextFrame >= this.meta.clip_total_frames) {
                nextFrame = 0; // Loop
            }
            this.updateFrame(nextFrame, true); // Force UI update during playback
        }, intervalTime);
    }

    pause() {
        if (!this.isPlaying) return;
        clearInterval(this.playbackInterval);
        this.playbackInterval = null;
        this.isPlaying = false;
        if (this.playPauseBtn) this.playPauseBtn.textContent = '⏯️';
        console.log(`[Player ${this.clipId}] Playback Paused`);
    }

    // Public method to get current frame
    getCurrentFrame() {
        return this.currentFrame;
    }

    // Cleanup method
    cleanup() {
        this.pause(); // Ensure interval is cleared
        // Remove event listeners if necessary (though often handled by element removal)
        console.log(`[Player ${this.clipId}] Cleaned up.`);
    }
}