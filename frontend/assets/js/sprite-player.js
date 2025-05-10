/**
 * Sprite-sheet utilities for Clip Review
 * ──────────────────────────────────────────────────────────────────────────
 * • SpritePlayerController  – full scrubbable viewer in the main panel
 * • SplitManager            – global split-mode state machine
 * • ThumbHoverPlayer        – lightweight hover-autoplay for sibling thumbs
 *
 * Register all three hooks with LiveSocket:
 *
 *     import { SpritePlayerController,
 *              SplitManager,
 *              ThumbHoverPlayer } from "./sprite-player"
 *
 *     let Hooks = {
 *       ReviewHotkeys,
 *       SpritePlayer: SpritePlayerController,
 *       ThumbHoverPlayer
 *     }
 *
 * No server round-trips: everything runs in the browser.
 */

/* ────────────────────────────────────────────────────────────────────────── */
/*  Split-mode state machine – global per page                              */
/* ────────────────────────────────────────────────────────────────────────── */

export const SplitManager = {
  splitMode     : false,
  activePlayer  : null,
  btnEl         : null,

  /** Enter split mode: pause playback, highlight UI. */
  enter(player, btn) {
    this.splitMode    = true;
    this.activePlayer = player;
    this.btnEl        = btn;

    player.pause("split-enter");
    btn?.classList.add("split-armed");
    player.viewerEl.classList.add("split-armed");
  },

  /** Exit without committing. */
  exit() {
    if (!this.splitMode) return;
    this.btnEl?.classList.remove("split-armed");
    this.activePlayer.viewerEl.classList.remove("split-armed");
    this.activePlayer.play("split-exit");

    this.splitMode    = false;
    this.activePlayer = null;
    this.btnEl        = null;
  },

  /** Commit the chosen frame – pushes a `"select"` event and resets state. */
  commit(pushFn) {
    if (!this.splitMode || !this.activePlayer) return;
    pushFn("select", {
      action: "split",
      frame : this.activePlayer.currentFrame
    });
    this.exit();
  },

  /** Nudge frame cursor left / right while armed. */
  nudge(delta) {
    if (!this.splitMode || !this.activePlayer) return;
    const next = this.activePlayer.currentFrame + delta;
    this.activePlayer.updateFrame(next, true);
  }
};

/* ────────────────────────────────────────────────────────────────────────── */
/*  Phoenix Hook – full sprite player                                       */
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
    try { meta = JSON.parse(playerData); }
    catch (err) {
      console.error("[SpritePlayer] invalid JSON in data-player", err);
      return;
    }
    if (!meta.isValid) {
      console.error("[SpritePlayer] meta not valid", meta);
      return;
    }

    /* collect control elements */
    const container  = this.el.parentElement;
    const scrub      = container.querySelector(`#scrub-${clipId}`);
    const playPause  = container.querySelector(`#playpause-${clipId}`);
    const frameLabel = container.querySelector(`#frame-display-${clipId}`);
    const splitBtn   = document.getElementById(`split-${clipId}`);

    if (!scrub || !playPause || !frameLabel) {
      console.warn("[SpritePlayer] missing controls for clip", clipId);
      return;
    }

    /* instantiate player */
    this.player = new SpritePlayer(
      clipId,
      this.el,
      scrub,
      playPause,
      frameLabel,
      meta
    );

    /* Split-button */
    splitBtn?.addEventListener("click", () => {
      if (!SplitManager.splitMode) {
        SplitManager.enter(this.player, splitBtn);
      } else {
        SplitManager.commit((evt, payload) => this.pushEvent(evt, payload));
      }
    });

    /* Click on sprite area toggles play/pause */
    this._onViewerClick = () => this.player.togglePlayback();
    this.el.addEventListener("click", this._onViewerClick);

    /* Global keyboard shortcuts */
    this._onKey = evt => {
      /* Space (plain) → play/pause toggle */
      if (evt.code === "Space" && !evt.shiftKey && !evt.metaKey && !evt.ctrlKey) {
        evt.preventDefault();
        this.player.togglePlayback();
        return;
      }

      /* Arrow keys drive split mode */
      if (evt.key === "ArrowLeft" || evt.key === "ArrowRight") {
        evt.preventDefault();
        if (!SplitManager.splitMode) SplitManager.enter(this.player, splitBtn);
        SplitManager.nudge(evt.key === "ArrowLeft" ? -1 : +1);
        return;
      }

      if (evt.key === "Escape") SplitManager.exit();
    };
    window.addEventListener("keydown", this._onKey);

    /* kick off playback */
    this.player.preloadAndPlay();
  },

  updated() {
    if (this.player) this.player.cleanup();
    this.mounted();                     // re-init on DOM patch
  },

  destroyed() {
    this.player?.cleanup();
    this.el.removeEventListener("click", this._onViewerClick);
    window.removeEventListener("keydown", this._onKey);
  }
};

/* ────────────────────────────────────────────────────────────────────────── */
/*  Phoenix Hook – hover-autoplay thumbnail                                 */
/* ────────────────────────────────────────────────────────────────────────── */

export const ThumbHoverPlayer = {
  mounted() {
    const cfg = JSON.parse(this.el.dataset.player);

    /* geometry – scale every thumb down to 160 px wide,
       preserving aspect ratio */
    const THUMB_W = 160;
    this.cols   = cfg.cols;
    this.rows   = cfg.rows;
    const scale = THUMB_W / cfg.tile_width;
    this.w      = THUMB_W;
    this.h      = Math.round(cfg.tile_height_calculated * scale);

    this.total = cfg.total_sprite_frames;
    /* playback speed = same fps as the big player (capped at 60) */
    this.fps   = Math.min(60, cfg.clip_fps || 24);

    this.frame = 0;
    this.timer = null;

    /* style element */
    Object.assign(this.el.style, {
      width:             `${this.w}px`,
      height:            `${this.h}px`,
      backgroundImage:   `url("${cfg.spriteUrl}")`,
      backgroundRepeat:  "no-repeat",
      backgroundSize:    `${this.w * this.cols}px auto`,
      backgroundPosition:"0 0",
      cursor:            "pointer"
    });

    /* now wire up hover playback */
    this.el.addEventListener("mouseenter", () => this.play());
    this.el.addEventListener("mouseleave", () => this.stop());
  },

  play() {
    if (this.timer) return;
    const interval = 1000 / this.fps;
    this.timer = setInterval(() => this.step(), interval);
  },

  stop() {
    clearInterval(this.timer);
    this.timer = null;
    this.frame = 0;
    this.updateBackground();
  },

  step() {
    this.frame = (this.frame + 1) % this.total;
    this.updateBackground();
  },

  updateBackground() {
    const col = this.frame % this.cols;
    const row = Math.floor(this.frame / this.cols);
    this.el.style.backgroundPosition = `-${col * this.w}px -${row * this.h}px`;
  },

  destroyed() {
    this.stop();
  }
};

/* ────────────────────────────────────────────────────────────────────────── */
/*  Minimal sprite-sheet “video” player (used by main panel)                */
/* ────────────────────────────────────────────────────────────────────────── */

class SpritePlayer {
  constructor(clipId, viewerEl, scrubEl, playPauseBtn, frameLblEl, meta) {
    /* element refs */
    this.clipId       = clipId;
    this.viewerEl     = viewerEl;
    this.scrubEl      = scrubEl;
    this.playPauseBtn = playPauseBtn;
    this.frameLblEl   = frameLblEl;

    /* metadata */
    this.meta    = meta;

    /* runtime state */
    this.currentFrame     = 1;     // avoid padding pixel in frame 0
    this.isPlaying        = false;
    this.playbackInterval = null;
    this.isScrubbing      = false;

    /* preload sprite PNG */
    this.spriteImage      = new Image();
    this.spriteImage.src  = meta.spriteUrl;

    this._setupUI();
    this._attachEventListeners();
  }

  /* initialise DOM attributes ------------------------------------------ */
  _setupUI() {
    const m = this.meta;

    /* viewer background – gigantic sheet of tiles */
    Object.assign(this.viewerEl.style, {
      backgroundImage:   `url('${m.spriteUrl}')`,
      backgroundSize:    `${m.cols * m.tile_width}px ${m.rows * m.tile_height_calculated}px`,
      width:             `${m.tile_width}px`,
      height:            `${m.tile_height_calculated}px`,
      backgroundRepeat:  "no-repeat",
      backgroundPosition:"0 0"
    });

    /* controls default state */
    this.scrubEl.min      = 1;
    this.scrubEl.max      = Math.max(1, m.clip_total_frames - 1);
    this.scrubEl.value    = this.currentFrame;
    this.scrubEl.disabled = false;

    this.playPauseBtn.disabled    = false;
    this.playPauseBtn.textContent = "▶";

    this.frameLblEl.textContent = `Frame: ${this.currentFrame}`;
  }

  /* event listeners ----------------------------------------------------- */
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

  /* public API ---------------------------------------------------------- */

  /** preload sprite then commence autoplay. */
  preloadAndPlay() {
    if (this.spriteImage.complete) {
      this.play("mounted");
    } else {
      this.spriteImage.onload = () => this.play("preload");
    }
  }

  /** jump to frame N (clamped). */
  updateFrame(frameNum, force = false) {
    const last = this.meta.clip_total_frames - 1;
    const f    = Math.max(1, Math.min(frameNum, last));
    if (f === this.currentFrame && !force) return;

    this.currentFrame = f;

    const { bgX, bgY } = this._bgPosForFrame(f);
    this.viewerEl.style.backgroundPosition = `${bgX}px ${bgY}px`;

    this.frameLblEl.textContent = `Frame: ${f}`;
    if (!this.isScrubbing) this.scrubEl.value = f;
  }

  play() {
    if (this.isPlaying || this.meta.clip_fps <= 0) return;
    this.isPlaying = true;
    this.playPauseBtn.textContent = "⏸";

    const interval = 1000 / this.meta.clip_fps;
    const last     = this.meta.clip_total_frames - 1;
    this.playbackInterval = setInterval(() => {
      const nxt = (this.currentFrame + 1 > last) ? 1 : this.currentFrame + 1;
      this.updateFrame(nxt, true);
    }, interval);
  }

  pause() {
    if (!this.isPlaying) return;
    this.isPlaying = false;
    this.playPauseBtn.textContent = "▶";
    clearInterval(this.playbackInterval);
    this.playbackInterval = null;
  }

  togglePlayback() { this.isPlaying ? this.pause() : this.play(); }

  /** clean up DOM listeners for LV patch / teardown. */
  cleanup() {
    this.pause();
    this.playPauseBtn.removeEventListener("click", this._onPlayPause);
    this.scrubEl.removeEventListener("mousedown", this._onScrubStart);
    this.scrubEl.removeEventListener("mouseup",   this._onScrubEnd);
    this.scrubEl.removeEventListener("input",     this._onScrubInput);
  }

  /* internals ----------------------------------------------------------- */

  /** calculate CSS background offset for a given frame. */
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