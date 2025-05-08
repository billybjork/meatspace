export const SpritePlayerController = {
  mounted() {
    const clipId     = this.el.dataset.clipId;
    const playerData = this.el.dataset.player;
    if (!playerData) {
      console.warn("[SpritePlayer] missing data-player", this.el);
      return;
    }

    let meta;
    try {
      meta = JSON.parse(playerData);
    } catch (err) {
      console.error("[SpritePlayer] invalid JSON in data-player", err);
      return;
    }
    if (!meta.isValid) {
      console.error("[SpritePlayer] meta not valid", meta);
      return;
    }

    const container  = this.el.parentElement.closest('.clip-display-container') || this.el.parentElement; // Be more robust finding container
    const scrub      = container.querySelector(`#scrub-${clipId}`);
    const playPause  = container.querySelector(`#playpause-${clipId}`);
    const frameLabel = container.querySelector(`#frame-display-${clipId}`);

    if (!scrub || !playPause || !frameLabel) {
      console.warn("[SpritePlayer] missing controls for clip", clipId, {scrub, playPause, frameLabel});
      return;
    }

    this.player = new SpritePlayer(
      clipId,
      this.el, // This is the viewerEl
      scrub,
      playPause,
      frameLabel,
      meta,
      // Pass a function that uses LiveSocket's pushEventTo
      (event_name, payload) => {
        // 'this' inside pushEventTo needs to be the LiveView component instance.
        // We target the closest [phx-view] which should be the ReviewLive instance.
        // The target for pushEventTo is the DOM element of the LiveView itself.
        const liveViewElement = this.el.closest("[phx-view]");
        if (liveViewElement) {
            this.pushEventTo(liveViewElement, event_name, payload);
        } else {
            console.error("[SpritePlayer] Could not find [phx-view] to push event to.");
        }
      }
    );

    // Listen for events dispatched from the LiveView to this specific hook instance
    this.handleSetSplitMode = (e) => {
        if (this.player) { // Ensure player exists
            this.player.setSplitMode(e.detail.active, e.detail.frame);
        }
    };
    this.handleAdjustFrame = (e) => {
        if (this.player && this.player.splitModeActive) {
            this.player.adjustFrameByDelta(e.detail.direction === "next" ? 1 : -1);
        }
    };

    this.el.addEventListener("set_player_split_mode", this.handleSetSplitMode);
    this.el.addEventListener("adjust_player_frame", this.handleAdjustFrame); // Listen for arrow key adjustments

    // preload & then auto-play
    if (this.player) this.player.preloadAndPlay();
  },

  updated() {
    // This is called when the LiveView re-renders this component.
    // We need to clean up the old player and its listeners, then re-mount.
    console.log("[SpritePlayer] updated() called for clip:", this.el.dataset.clipId);
    if (this.el && this.handleSetSplitMode) {
        this.el.removeEventListener("set_player_split_mode", this.handleSetSplitMode);
    }
    if (this.el && this.handleAdjustFrame) {
        this.el.removeEventListener("adjust_player_frame", this.handleAdjustFrame);
    }
    if (this.player) {
        this.player.cleanup();
        this.player = null; // Explicitly nullify
    }
    this.mounted(); // Re-run mount logic to create a new player for the new/updated clip
  },

  destroyed() {
    console.log("[SpritePlayer] destroyed() called for clip:", this.el.dataset.clipId);
    if (this.el && this.handleSetSplitMode) {
        this.el.removeEventListener("set_player_split_mode", this.handleSetSplitMode);
    }
    if (this.el && this.handleAdjustFrame) {
        this.el.removeEventListener("adjust_player_frame", this.handleAdjustFrame);
    }
    if (this.player) {
        this.player.cleanup();
        this.player = null;
    }
  }
};

class SpritePlayer {
  constructor(clipId, viewerEl, scrubEl, playPauseBtn, frameLblEl, meta, pushEventCallback) {
    this.clipId          = clipId;
    this.viewerEl        = viewerEl;
    this.scrubEl         = scrubEl;
    this.playPauseBtn    = playPauseBtn;
    this.frameLblEl      = frameLblEl;
    this.meta            = meta;
    this.pushEventCallback = pushEventCallback;

    this.currentFrame     = 0;
    this.isPlaying        = false;
    this.playbackInterval = null;
    this.isScrubbing      = false;
    this.splitModeActive  = false; // New state for split mode

    // create our Image object for preloading
    this.spriteImage = new Image();
    if (meta && meta.spriteUrl) { // Guard against meta being incomplete
        this.spriteImage.src = meta.spriteUrl;
    }


    this._setupUI();
    this._attachEventListeners();
    console.log(`[SpritePlayer ${this.clipId}] Initialized. Split mode: ${this.splitModeActive}`);
  }

  /** Preload the sprite image, then start playback once loaded. */
  preloadAndPlay() {
    if (!this.spriteImage) return; // Guard if image object not created

    if (this.spriteImage.complete && this.spriteImage.src) { // Check src to avoid playing for placeholder if URL was bad
      this.play("mounted");
    } else if (this.spriteImage.src) { // Only set onload if src is valid
      this.spriteImage.onload = () => this.play("preload");
      this.spriteImage.onerror = () => console.error(`[SpritePlayer ${this.clipId}] Error loading sprite image: ${this.spriteImage.src}`);
    } else {
        console.warn(`[SpritePlayer ${this.clipId}] No valid spriteUrl to preload.`);
    }
  }

  _setupUI() {
    if (!this.meta || !this.viewerEl || !this.scrubEl || !this.playPauseBtn || !this.frameLblEl) {
        console.warn(`[SpritePlayer ${this.clipId}] Missing meta or UI elements in _setupUI.`);
        return;
    }
    const m = this.meta;
    this.viewerEl.style.backgroundImage = `url('${m.spriteUrl}')`;
    this.viewerEl.style.backgroundSize =
      `${m.cols * m.tile_width}px ${m.rows * m.tile_height_calculated}px`;
    this.viewerEl.style.width  = `${m.tile_width}px`;
    this.viewerEl.style.height = `${m.tile_height_calculated}px`;
    this.viewerEl.style.backgroundRepeat = 'no-repeat';
    this.viewerEl.style.backgroundPosition = '0 0';

    this.scrubEl.max      = Math.max(0, m.clip_total_frames - 1);
    this.scrubEl.value    = 0;
    this.scrubEl.disabled = false; // Will be controlled by split_mode_active

    this.playPauseBtn.disabled    = false; // Will be controlled by split_mode_active
    this.playPauseBtn.textContent = '⏯️';

    this.frameLblEl.textContent = `Frame: 0`;
    this.updateFrame(0, true); // Ensure initial frame position is set
  }

  _attachEventListeners() {
    if (!this.playPauseBtn || !this.scrubEl) {
        console.warn(`[SpritePlayer ${this.clipId}] Missing playPauseBtn or scrubEl in _attachEventListeners.`);
        return;
    }

    this._onPlayPause = e => {
      e.stopPropagation();
      if (!this.splitModeActive) { // Only allow play/pause if not in split mode
          this.togglePlayback();
      }
    };
    this.playPauseBtn.addEventListener('click', this._onPlayPause);

    this._onScrubStart = () => {
      this.isScrubbing = true;
      if (this.isPlaying) this.pause("scrubStart");
      // If split mode is not active when scrubbing starts, LV might activate it.
      // For now, just ensure the player itself knows about scrubbing.
    };
    this._onScrubEnd = () => {
      this.isScrubbing = false;
      // If split mode was activated by scrubbing, this.splitModeActive would be true.
      // The frame update via input event will have sent the frame to LV.
    };
    this._onScrubInput = e => {
      const f = parseInt(e.target.value, 10);
      this.updateFrame(f, true); // This will now inform LV if in split mode
    };
    this.scrubEl.addEventListener('mousedown', this._onScrubStart);
    this.scrubEl.addEventListener('mouseup',   this._onScrubEnd);
    this.scrubEl.addEventListener('input',     this._onScrubInput);
  }

  /** Map clip‐frame → background offsets, ensuring we never overshoot. */
  _calculateBGPosition(frameNum) {
    if (!this.meta) return { bgX: 0, bgY: 0}; // Guard
    const m            = this.meta;
    const maxClipFrame = Math.max(0, m.clip_total_frames - 1);

    const usable = Math.min(m.clip_total_frames, m.total_sprite_frames);
    const prop = maxClipFrame > 0 ? frameNum / maxClipFrame : 0;
    const target = prop * (usable - 1);
    const spriteIndex = Math.floor(target);

    const col = spriteIndex % m.cols;
    const row = Math.floor(spriteIndex / m.cols);

    return {
      bgX: -(col * m.tile_width),
      bgY: -(row * m.tile_height_calculated)
    };
  }

  setSplitMode(isActive, initialFrame = null) {
    if (!this.viewerEl || !this.playPauseBtn) { // Guard elements
        console.warn(`[SpritePlayer ${this.clipId}] viewerEl or playPauseBtn not available in setSplitMode.`);
        return;
    }
    this.splitModeActive = isActive;
    console.log(`[SpritePlayer ${this.clipId}] setSplitMode called. Active: ${isActive}, Initial Frame: ${initialFrame}`);
    this.viewerEl.classList.toggle("split-mode-active-viewer", isActive);

    if (isActive) {
        this.pause("split_mode_enter");
        this.playPauseBtn.disabled = true; // Disable play/pause button
        // this.scrubEl.disabled = false; // Scrub bar should remain active

        if (initialFrame !== null && !isNaN(parseInt(initialFrame))) {
            this.updateFrame(parseInt(initialFrame), true); // Force update and send to LV
        } else {
            // If no initial frame, ensure current frame is sent to LV
            this.updateFrame(this.currentFrame, true);
        }
    } else {
        this.playPauseBtn.disabled = false; // Re-enable play/pause
        // this.scrubEl.disabled = false; // Ensure scrub bar is enabled
        // No need to auto-play, user can do it.
    }
  }

  adjustFrameByDelta(delta) {
    if (!this.splitModeActive || !this.meta || this.meta.clip_total_frames === undefined) return;
    const newFrame = Math.max(0, Math.min(this.currentFrame + delta, this.meta.clip_total_frames - 1));
    this.updateFrame(newFrame, true); // Force update and send to LV
  }

  updateFrame(frameNum, force = false) {
    if (!this.meta || this.meta.clip_total_frames === undefined || isNaN(frameNum)) {
        // console.warn(`[SpritePlayer ${this.clipId}] updateFrame called with invalid meta or frameNum: ${frameNum}`);
        return;
    }
    const f = Math.max(0, Math.min(frameNum, this.meta.clip_total_frames - 1));

    if (f === this.currentFrame && !force) return;
    this.currentFrame = f;

    if (this.viewerEl) {
      const { bgX, bgY } = this._calculateBGPosition(f);
      this.viewerEl.style.backgroundPosition = `${bgX}px ${bgY}px`;
    }

    if (this.frameLblEl) this.frameLblEl.textContent = `Frame: ${f}`;
    if (this.scrubEl && !this.isScrubbing) this.scrubEl.value = f;

    // If in split mode, send the updated frame to the LiveView
    if (this.splitModeActive && typeof this.pushEventCallback === 'function') {
      // console.log(`[SpritePlayer ${this.clipId}] Pushing split_frame_update, frame: ${this.currentFrame}`);
      this.pushEventCallback("split_frame_update", { frame: this.currentFrame, clip_id: this.clipId });
    }
  }

  play(source = "unknown") {
    if (this.isPlaying || !this.meta || this.meta.clip_fps <= 0 || this.splitModeActive || !this.playPauseBtn) return;
    this.isPlaying = true;
    this.playPauseBtn.textContent = '⏸️';

    if (this.playbackInterval) clearInterval(this.playbackInterval);

    const intervalMs = 1000 / this.meta.clip_fps;
    this.playbackInterval = setInterval(() => {
      if (!this.meta || this.meta.clip_total_frames === undefined) {
          clearInterval(this.playbackInterval);
          return;
      }
      const nextFrame = this.currentFrame + 1;
      const newFrame = nextFrame >= this.meta.clip_total_frames ? 0 : nextFrame;
      this.updateFrame(newFrame, true); // Force update as it's continuous play
    }, intervalMs);
  }

  pause(source = "unknown") {
    if (!this.isPlaying && this.playbackInterval === null) return; // Already paused or never started
    this.isPlaying = false;
    if (this.playPauseBtn) this.playPauseBtn.textContent = '⏯️'; // Check if button exists
    clearInterval(this.playbackInterval);
    this.playbackInterval = null;
    // console.log(`[SpritePlayer ${this.clipId}] Paused. Source: ${source}`);
  }

  togglePlayback() {
    if (this.splitModeActive) return; // Don't toggle if in split mode
    this.isPlaying ? this.pause("toggle") : this.play("toggle");
  }

  cleanup() {
    console.log(`[SpritePlayer ${this.clipId}] Cleaning up.`);
    this.pause("cleanup"); // This also clears the interval
    if (this.spriteImage) {
        this.spriteImage.onload = null; // Remove onload handler
        this.spriteImage.onerror = null;
        this.spriteImage.src = ""; // Help GC
        this.spriteImage = null;
    }
    if (this.playPauseBtn && this._onPlayPause) {
        this.playPauseBtn.removeEventListener('click', this._onPlayPause);
    }
    if (this.scrubEl) {
        if (this._onScrubStart) this.scrubEl.removeEventListener('mousedown', this._onScrubStart);
        if (this._onScrubEnd) this.scrubEl.removeEventListener('mouseup',   this._onScrubEnd); // Corrected typo from original: _onScubEnd
        if (this._onScrubInput) this.scrubEl.removeEventListener('input',     this._onScrubInput);
    }

    // Clear references for GC
    this.viewerEl = null;
    this.scrubEl = null;
    this.playPauseBtn = null;
    this.frameLblEl = null;
    this.meta = null;
    this.pushEventCallback = null;
  }
}