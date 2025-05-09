/**
 * Phoenix LiveView hook that turns a sprite-sheet PNG into a lightweight,
 * scrubbable “video” player.  Includes keyboard-driven split-mode for
 * manually selecting a cut-frame.
 *
 * All logic is self-contained so the LiveView only needs to:
 *   * emit  <div phx-hook="SpritePlayer" …>
 *   * listen for `"select"` events
 */

/* ────────────────────────────────────────────────────────────────────────── */
/*  Split-mode state machine – global per page                              */
/* ────────────────────────────────────────────────────────────────────────── */

export const SplitManager = {
  splitMode     : false,   // are we armed?
  activePlayer  : null,    // SpritePlayer instance currently controlled
  btnEl         : null,    // the button that toggled us

  /**
   * Enter split mode: pause playback, highlight UI.
   */
  enter(player, btn) {
    this.splitMode    = true;
    this.activePlayer = player;
    this.btnEl        = btn;

    player.pause("split-enter");
    btn.classList.add("split-armed");
    player.viewerEl.classList.add("split-armed");
  },

  /**
   * Exit without committing.
   */
  exit() {
    if (!this.splitMode) return;
    this.btnEl.classList.remove("split-armed");
    this.activePlayer.viewerEl.classList.remove("split-armed");
    this.activePlayer.play("split-exit");

    this.splitMode    = false;
    this.activePlayer = null;
    this.btnEl        = null;
  },

  /**
   * Commit the chosen frame – pushes a `"select"` event and resets state.
   */
  commit(pushFn) {
    if (!this.splitMode || !this.activePlayer) return;
    pushFn("select", {
      action: "split",
      frame : this.activePlayer.currentFrame
    });
    this.exit();
  },

  /**
   * Nudge frame cursor left / right while armed.
   */
  nudge(delta) {
    if (!this.splitMode || !this.activePlayer) return;
    const next = this.activePlayer.currentFrame + delta;
    this.activePlayer.updateFrame(next, true);
  }
};

/* ────────────────────────────────────────────────────────────────────────── */
/*  Phoenix Hook – one instance per sprite-sheet viewer                     */
/* ────────────────────────────────────────────────────────────────────────── */

export const SpritePlayerController = {
  /* --------------------------------------------------------------------- */
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

    /* Collect control elements */
    const container  = this.el.parentElement;
    const scrub      = container.querySelector(`#scrub-${clipId}`);
    const playPause  = container.querySelector(`#playpause-${clipId}`);
    const frameLabel = container.querySelector(`#frame-display-${clipId}`);
    const splitBtn   = document.getElementById(`split-${clipId}`);

    if (!scrub || !playPause || !frameLabel) {
      console.warn("[SpritePlayer] missing controls for clip", clipId);
      return;
    }

    /* Instantiate player */
    this.player = new SpritePlayer(
      clipId,
      this.el,
      scrub,
      playPause,
      frameLabel,
      meta,
      null // split-callback no longer used, kept for BC
    );

    /* Split-button behaviour */
    if (splitBtn) {
      splitBtn.addEventListener("click", () => {
        if (!SplitManager.splitMode) {
          SplitManager.enter(this.player, splitBtn);
        } else {
          SplitManager.commit((evt, payload) => this.pushEvent(evt, payload));
        }
      });
    }

    // Click on the sprite area toggles play/pause
    this._onViewerClick = () => {
      this.player.togglePlayback();
    };
    this.el.addEventListener("click", this._onViewerClick);

    /* Global keyboard shortcuts while viewer is mounted
     * --------------------------------------------------
     *  ⬅️ / ➡️   – if split-mode is *not* armed yet, first enter
     *             split-mode (same effect as clicking ✂️) and then
     *             nudge one frame.  If already armed, just nudge.
     *
     *  Esc      – exit split-mode and resume playback.
     * -------------------------------------------------- */
    this._onKey = evt => {
      // Space → play/pause toggle
      if (evt.code === "Space") {
        evt.preventDefault();
        this.player.togglePlayback();
        return;
      }

      if (evt.key === "ArrowLeft" || evt.key === "ArrowRight") {
        evt.preventDefault();                    // stop page scroll

        if (!SplitManager.splitMode) {
          /* auto-arm split mode on first arrow press */
          SplitManager.enter(this.player, splitBtn);
        }
        SplitManager.nudge(evt.key === "ArrowLeft" ? -1 : +1);
        return;
      }

      if (evt.key === "Escape") {
        SplitManager.exit();
      }
    };
    
    window.addEventListener("keydown", this._onKey);

    /* Kick off playback once sprite is loaded */
    this.player.preloadAndPlay();
  },

  /* LiveView DOM patch – re-mount in place */
  updated() {
    if (this.player) this.player.cleanup();
    this.mounted();
  },

  destroyed() {
    if (this.player) this.player.cleanup();
    if (this._onViewerClick) this.el.removeEventListener("click", this._onViewerClick);
    if (this._onKey) window.removeEventListener("keydown", this._onKey);
  }
};

/* ────────────────────────────────────────────────────────────────────────── */
/*  Minimal sprite-sheet “player”                                           */
/* ────────────────────────────────────────────────────────────────────────── */

class SpritePlayer {
  constructor(clipId, viewerEl, scrubEl, playPauseBtn, frameLblEl, meta, splitCb) {
    /* element refs */
    this.clipId       = clipId;
    this.viewerEl     = viewerEl;
    this.scrubEl      = scrubEl;
    this.playPauseBtn = playPauseBtn;
    this.frameLblEl   = frameLblEl;

    /* metadata */
    this.meta    = meta;
    this.splitCb = splitCb; // kept for backward-compat but unused

    /* runtime state */
    // Start at frame 1 to avoid the padding pixel in frame 0
    this.currentFrame     = 1;
    this.isPlaying        = false;
    this.playbackInterval = null;
    this.isScrubbing      = false;

    /* preload sprite PNG */
    this.spriteImage      = new Image();
    this.spriteImage.src  = meta.spriteUrl;

    this._setupUI();
    this._attachEventListeners();
  }

  /* --------------------------------------------------------------------- */
  /*  Initialise DOM attributes                                            */
  /* --------------------------------------------------------------------- */
  _setupUI() {
    const m = this.meta;

    /* viewer background – single, gigantic sheet of tiles */
    this.viewerEl.style.backgroundImage    = `url('${m.spriteUrl}')`;
    this.viewerEl.style.backgroundSize     = `${m.cols * m.tile_width}px ${m.rows * m.tile_height_calculated}px`;
    this.viewerEl.style.width              = `${m.tile_width}px`;
    this.viewerEl.style.height             = `${m.tile_height_calculated}px`;
    this.viewerEl.style.backgroundRepeat   = "no-repeat";
    this.viewerEl.style.backgroundPosition = "0 0";

    /* controls default state */
    // Prevent displaying frame 0
    this.scrubEl.min      = 1;
    this.scrubEl.max      = Math.max(1, m.clip_total_frames - 1);
    this.scrubEl.value    = this.currentFrame;
    this.scrubEl.disabled = false;

    this.playPauseBtn.disabled    = false;
    this.playPauseBtn.textContent = "▶";

    this.frameLblEl.textContent = `Frame: ${this.currentFrame}`;
  }

  /* --------------------------------------------------------------------- */
  /*  Event listeners                                                      */
  /* --------------------------------------------------------------------- */
  _attachEventListeners() {
    /* play / pause */
    this._onPlayPause = e => {
      e.stopPropagation();
      this.togglePlayback();
    };
    this.playPauseBtn.addEventListener("click", this._onPlayPause);

    /* scrub bar */
    this._onScrubStart = () => {
      this.isScrubbing = true;
      if (this.isPlaying) this.pause("scrubStart");
    };
    this._onScrubEnd   = () => { this.isScrubbing = false; };
    this._onScrubInput = e => {
      const f = parseInt(e.target.value, 10);
      this.updateFrame(f, true);
    };
    this.scrubEl.addEventListener("mousedown", this._onScrubStart);
    this.scrubEl.addEventListener("mouseup",   this._onScrubEnd);
    this.scrubEl.addEventListener("input",     this._onScrubInput);
  }

  /* --------------------------------------------------------------------- */
  /*  Public API                                                           */
  /* --------------------------------------------------------------------- */

  /** Preload sprite then commence autoplay. */
  preloadAndPlay() {
    if (this.spriteImage.complete) {
      this.play("mounted");
    } else {
      this.spriteImage.onload = () => this.play("preload");
    }
  }

  /** Jump to frame N (clamped). */
  updateFrame(frameNum, force = false) {
    // clamp to [1 … last] so frame 0 is never displayed
    const last = this.meta.clip_total_frames - 1;
    const f    = Math.max(1, Math.min(frameNum, last));
    if (f === this.currentFrame && !force) return;

    this.currentFrame = f;

    const { bgX, bgY } = this._bgPosForFrame(f);
    this.viewerEl.style.backgroundPosition = `${bgX}px ${bgY}px`;

    if (this.frameLblEl) this.frameLblEl.textContent = `Frame: ${f}`;
    if (this.scrubEl && !this.isScrubbing) this.scrubEl.value = f;
  }

  play(src = "unknown") {
    if (this.isPlaying || this.meta.clip_fps <= 0) return;
    this.isPlaying = true;
    this.playPauseBtn.textContent = "⏸";

    /* advance at source FPS */
    const interval = 1000 / this.meta.clip_fps;
    const last     = this.meta.clip_total_frames - 1;
    this.playbackInterval = setInterval(() => {
      // wrap from “last” straight back to 1 (skips 0)
      const nxt = (this.currentFrame + 1 > last) ? 1 : this.currentFrame + 1;
      this.updateFrame(nxt, true);
    }, interval);
  }

  pause(src = "unknown") {
    if (!this.isPlaying) return;
    this.isPlaying = false;
    this.playPauseBtn.textContent = "▶";
    clearInterval(this.playbackInterval);
    this.playbackInterval = null;
  }

  togglePlayback() {
    this.isPlaying ? this.pause("toggle") : this.play("toggle");
  }

  /** Clean up DOM listeners for LV patch / teardown. */
  cleanup() {
    this.pause("cleanup");
    this.playPauseBtn.removeEventListener("click", this._onPlayPause);
    this.scrubEl.removeEventListener("mousedown", this._onScrubStart);
    this.scrubEl.removeEventListener("mouseup",   this._onScrubEnd);
    this.scrubEl.removeEventListener("input",     this._onScrubInput);

    /* help GC */
    this.viewerEl = this.scrubEl = this.playPauseBtn = this.frameLblEl = this.meta = null;
  }

  /* --------------------------------------------------------------------- */
  /*  Internals                                                            */
  /* --------------------------------------------------------------------- */

  /** Calculate CSS background offset for a given frame. */
  _bgPosForFrame(frameNum) {
    const m            = this.meta;
    const spriteFrames = m.total_sprite_frames;

    const prop   = (spriteFrames > 1) ? frameNum / (m.clip_total_frames - 1) : 0;
    const index  = Math.floor(prop * (spriteFrames - 1));

    const col    = index % m.cols;
    const row    = Math.floor(index / m.cols);

    return {
      bgX: -(col * m.tile_width),
      bgY: -(row * m.tile_height_calculated)
    };
  }
}