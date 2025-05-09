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

    const container  = this.el.parentElement;
    const scrub      = container.querySelector(`#scrub-${clipId}`);
    const playPause  = container.querySelector(`#playpause-${clipId}`);
    const frameLabel = container.querySelector(`#frame-display-${clipId}`);
    if (!scrub || !playPause || !frameLabel) {
      console.warn("[SpritePlayer] missing controls for clip", clipId);
      return;
    }

    this.player = new SpritePlayer(
      clipId,
      this.el,
      scrub,
      playPause,
      frameLabel,
      meta,
      null
    );

    // preload & then auto-play
    this.player.preloadAndPlay();
  },

  updated() {
    if (this.player) this.player.cleanup();
    this.mounted();
  },

  destroyed() {
    if (this.player) this.player.cleanup();
  }
};

class SpritePlayer {
  constructor(clipId, viewerEl, scrubEl, playPauseBtn, frameLblEl, meta, splitCb) {
    this.clipId          = clipId;
    this.viewerEl        = viewerEl;
    this.scrubEl         = scrubEl;
    this.playPauseBtn    = playPauseBtn;
    this.frameLblEl      = frameLblEl;
    this.meta            = meta;
    this.splitCb         = splitCb;

    this.currentFrame     = 0;
    this.isPlaying        = false;
    this.playbackInterval = null;
    this.isScrubbing      = false;

    // create our Image object for preloading
    this.spriteImage = new Image();
    this.spriteImage.src = meta.spriteUrl;

    this._setupUI();
    this._attachEventListeners();
  }

  /** Preload the sprite image, then start playback once loaded. */
  preloadAndPlay() {
    if (this.spriteImage.complete) {
      this.play("mounted");
    } else {
      this.spriteImage.onload = () => this.play("preload");
      // optionally, handle onerror here too
    }
  }

  _setupUI() {
    const m = this.meta;
    // set background only once URL is known
    this.viewerEl.style.backgroundImage = `url('${m.spriteUrl}')`;
    this.viewerEl.style.backgroundSize =
      `${m.cols * m.tile_width}px ${m.rows * m.tile_height_calculated}px`;
    this.viewerEl.style.width  = `${m.tile_width}px`;
    this.viewerEl.style.height = `${m.tile_height_calculated}px`;
    this.viewerEl.style.backgroundRepeat = 'no-repeat';
    this.viewerEl.style.backgroundPosition = '0 0';

    // scrub bar
    this.scrubEl.max      = Math.max(0, m.clip_total_frames - 1);
    this.scrubEl.value    = 0;
    this.scrubEl.disabled = false;

    // play/pause button
    this.playPauseBtn.disabled    = false;
    this.playPauseBtn.textContent = '⏯️';

    // initial label
    this.frameLblEl.textContent = `Frame: 0`;
  }

  _attachEventListeners() {
    this._onPlayPause = e => {
      e.stopPropagation();
      this.togglePlayback();
    };
    this.playPauseBtn.addEventListener('click', this._onPlayPause);

    this._onScrubStart = () => {
      this.isScrubbing = true;
      if (this.isPlaying) this.pause("scrubStart");
    };
    this._onScrubEnd = () => {
      this.isScrubbing = false;
    };
    this._onScrubInput = e => {
      const f = parseInt(e.target.value, 10);
      this.updateFrame(f, true);
    };
    this.scrubEl.addEventListener('mousedown', this._onScrubStart);
    this.scrubEl.addEventListener('mouseup',   this._onScrubEnd);
    this.scrubEl.addEventListener('input',     this._onScrubInput);
  }

  /** Map clip‐frame → background offsets, ensuring we never overshoot. */
  _calculateBGPosition(frameNum) {
    const m            = this.meta;
    const maxClipFrame = Math.max(0, m.clip_total_frames - 1);

    // Cap to only the frames you actually generated in the sprite sheet
    const usable = Math.min(m.clip_total_frames, m.total_sprite_frames);

    // Proportion through the clip (0..1)
    const prop = maxClipFrame > 0 ? frameNum / maxClipFrame : 0;
    const target = prop * (usable - 1);

    // Math.floor guarantees you stay within valid tile indices
    const spriteIndex = Math.floor(target);

    const col = spriteIndex % m.cols;
    const row = Math.floor(spriteIndex / m.cols);

    return {
      bgX: -(col * m.tile_width),
      bgY: -(row * m.tile_height_calculated)
    };
  }

  updateFrame(frameNum, force = false) {
    const f = Math.max(0, Math.min(frameNum, this.meta.clip_total_frames - 1));
    if (f === this.currentFrame && !force) return;
    this.currentFrame = f;

    const { bgX, bgY } = this._calculateBGPosition(f);
    this.viewerEl.style.backgroundPosition = `${bgX}px ${bgY}px`;

    if (this.frameLblEl) this.frameLblEl.textContent = `Frame: ${f}`;
    if (this.scrubEl && !this.isScrubbing) this.scrubEl.value = f;

    if (typeof this.splitCb === 'function') {
      this.splitCb(this.clipId, f, this.meta);
    }
  }

  play(source = "unknown") {
    if (this.isPlaying || this.meta.clip_fps <= 0) return;
    this.isPlaying = true;
    this.playPauseBtn.textContent = '⏸️';

    // clear any old interval
    if (this.playbackInterval) clearInterval(this.playbackInterval);

    const intervalMs = 1000 / this.meta.clip_fps;
    this.playbackInterval = setInterval(() => {
      const next = this.currentFrame + 1 >= this.meta.clip_total_frames
        ? 0
        : this.currentFrame + 1;
      this.updateFrame(next, true);
    }, intervalMs);
  }

  pause(source = "unknown") {
    if (!this.isPlaying) return;
    this.isPlaying = false;
    this.playPauseBtn.textContent = '⏯️';
    clearInterval(this.playbackInterval);
    this.playbackInterval = null;
  }

  togglePlayback() {
    this.isPlaying ? this.pause("toggle") : this.play("toggle");
  }

  cleanup() {
    this.pause("cleanup");
    this.playPauseBtn.removeEventListener('click', this._onPlayPause);
    this.scrubEl.removeEventListener('mousedown', this._onScrubStart);
    this.scrubEl.removeEventListener('mouseup',   this._onScrubEnd);
    this.scrubEl.removeEventListener('input',     this._onScrubInput);
    // drop references for GC
    this.viewerEl = this.scrubEl = this.playPauseBtn = this.frameLblEl = this.meta = null;
  }
}  