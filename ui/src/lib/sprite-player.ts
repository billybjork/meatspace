export interface InternalSpriteMeta {
    tile_width: number;
    tile_height_calculated: number;
    cols: number;
    rows: number;
    total_sprite_frames: number;
    clip_fps_source: number;
    clip_total_frames_source: number;
    spriteUrl: string;
    isValid: boolean;
  }
  
  export default class SpritePlayer {
    private clipId: number;
    private viewerElement: HTMLDivElement;
    private scrubElement?: HTMLInputElement;
    private playPauseBtn?: HTMLButtonElement;
    private frameDisplayElement?: HTMLDivElement;
    private meta: InternalSpriteMeta;
    private updateSplitUICallback?: (clipId: number, frame: number, meta: InternalSpriteMeta) => void;
  
    private currentFrame: number = 0;
    private isPlaying: boolean = false;
    private playbackInterval: number | null = null;
    private isScrubbing: boolean = false;
    private isToggling: boolean = false;
  
    constructor(
      clipId: number,
      viewerElement: HTMLDivElement,
      scrubElement: HTMLInputElement | undefined,
      playPauseBtn: HTMLButtonElement | undefined,
      frameDisplayElement: HTMLDivElement | undefined,
      meta: InternalSpriteMeta,
      updateSplitUICallback?: (clipId: number, frame: number, meta: InternalSpriteMeta) => void
    ) {
      this.clipId = clipId;
      this.viewerElement = viewerElement;
      this.scrubElement = scrubElement;
      this.playPauseBtn = playPauseBtn;
      this.frameDisplayElement = frameDisplayElement;
      this.meta = meta;
      this.updateSplitUICallback = updateSplitUICallback;
  
      this.setupUI();
      this.attachEventListeners();
    }
  
    private setupUI() {
      if (!this.viewerElement || !this.meta.isValid) return;
  
      // background sprite
      this.viewerElement.style.backgroundImage = `url('${this.meta.spriteUrl}')`;
      const bgW = this.meta.cols * this.meta.tile_width;
      const bgH = this.meta.rows * this.meta.tile_height_calculated;
      this.viewerElement.style.backgroundSize = `${bgW}px ${bgH}px`;
      this.viewerElement.style.width = `${this.meta.tile_width}px`;
      this.viewerElement.style.height = `${this.meta.tile_height_calculated}px`;
      this.viewerElement.textContent = '';
      this.viewerElement.classList.remove('no-sprite');
  
      // scrub bar
      if (this.scrubElement) {
        const maxFrame = Math.max(0, this.meta.clip_total_frames_source - 1);
        this.scrubElement.max = String(maxFrame);
        this.scrubElement.value = '0';
        this.scrubElement.disabled = false;
      }
  
      // play/pause
      if (this.playPauseBtn) {
        this.playPauseBtn.disabled = false;
        this.playPauseBtn.textContent = '⏯️';
      }
  
      this.updateFrame(0, true);
    }
  
    private attachEventListeners() {
      if (this.playPauseBtn) {
        this.playPauseBtn.addEventListener('click', (e) => {
          e.stopPropagation();
          this.togglePlayback();
        });
      }
  
      if (this.scrubElement) {
        this.scrubElement.addEventListener('input', (e) => {
          const val = parseInt((e.target as HTMLInputElement).value, 10);
          this.updateFrame(val, true);
        });
        this.scrubElement.addEventListener('mousedown', () => {
          this.isScrubbing = true;
          if (this.isPlaying) this.pause();
        });
        this.scrubElement.addEventListener('mouseup', () => {
          this.isScrubbing = false;
        });
      }
    }
  
    private calculateBGPosition(frame: number): { bgX: number; bgY: number } {
      const maxClip = Math.max(0, this.meta.clip_total_frames_source - 1);
      const f = Math.max(0, Math.min(frame, maxClip));
      const proportion = maxClip > 0 ? f / maxClip : 0;
      const spriteIndex = Math.round(proportion * (this.meta.total_sprite_frames - 1));
      const col = spriteIndex % this.meta.cols;
      const row = Math.floor(spriteIndex / this.meta.cols);
      const x = -col * this.meta.tile_width;
      const y = -row * this.meta.tile_height_calculated;
      return { bgX: x, bgY: y };
    }
  
    public updateFrame(frame: number, forceUI: boolean = false) {
      if (!this.meta.isValid) return;
      const clamped = Math.round(Math.max(0, Math.min(frame, this.meta.clip_total_frames_source - 1)));
      if (clamped === this.currentFrame && !forceUI) return;
      this.currentFrame = clamped;
  
      const { bgX, bgY } = this.calculateBGPosition(clamped);
      this.viewerElement.style.backgroundPosition = `${bgX}px ${bgY}px`;
      if (this.frameDisplayElement) {
        this.frameDisplayElement.textContent = `Frame: ${clamped}`;
      }
      if (this.scrubElement && !this.isScrubbing) {
        this.scrubElement.value = String(clamped);
      }
      if (this.updateSplitUICallback) {
        this.updateSplitUICallback(this.clipId, clamped, this.meta);
      }
    }
  
    /**
     * Start playback. Accepts an optional source for debugging.
     */
    public play(source: string = 'unknown') {
      if (this.isPlaying || !this.meta.isValid || this.meta.clip_fps_source <= 0) return;
      this.isPlaying = true;
      const interval = 1000 / this.meta.clip_fps_source;
      this.playbackInterval = window.setInterval(() => this.updateFrame(this.currentFrame + 1), interval);
    }
  
    /**
     * Pause playback.
     */
    public pause() {
      if (!this.isPlaying) return;
      this.isPlaying = false;
      if (this.playbackInterval !== null) {
        clearInterval(this.playbackInterval);
        this.playbackInterval = null;
      }
    }
  
    /**
     * Toggle between play and pause.
     */
    public togglePlayback() {
      if (this.isToggling || !this.meta.isValid) return;
      this.isToggling = true;
      this.isPlaying ? this.pause() : this.play();
      setTimeout(() => (this.isToggling = false), 50);
    }

    public getCurrentFrame() {
        return this.currentFrame;
    }
  
    /**
     * Cleanup any active intervals or listeners.
     */
    public cleanup() {
      this.pause();
      // Additional teardown if needed
    }
  }