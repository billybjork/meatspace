import { onMount, onCleanup, createSignal } from 'solid-js';
import type { SpriteMeta } from '~/schemas/clip';
import { SpriteMetaSchema } from '~/schemas/clip';
import SpritePlayerClass from '~/lib/sprite-player';

export interface SpritePlayerProps {
  clipId: number;
  spriteUrl: string;
  spriteMeta: string; // JSON stringified metadata from API
  autoplay?: boolean;
  onSplitFrame?: (frame: number) => void;
  playerRef?: (instance: SpritePlayerClass) => void;
}

export default function SpritePlayer(props: SpritePlayerProps) {
  let viewerEl!: HTMLDivElement;
  let scrubEl!: HTMLInputElement;
  let playPauseBtn!: HTMLButtonElement;
  let frameDisplayEl!: HTMLDivElement;

  const [currentFrame, setCurrentFrame] = createSignal(0);
  let player: InstanceType<typeof SpritePlayerClass>;

  onMount(() => {
    // Parse and validate metadata
    const raw = JSON.parse(props.spriteMeta);
    const parsed: SpriteMeta = SpriteMetaSchema.parse(raw);

    // Map to the shape expected by SpritePlayerClass
    const meta = {
      tile_width: parsed.tile_width,
      tile_height_calculated: parsed.tile_height,
      cols: parsed.cols,
      rows: parsed.rows,
      total_sprite_frames: parsed.total_sprite_frames,
      clip_fps_source: parsed.clip_fps,
      clip_total_frames_source: parsed.clip_total_frames,
      spriteUrl: props.spriteUrl,
      isValid: true
    };

    player = new SpritePlayerClass(
      props.clipId,
      viewerEl,
      scrubEl,
      playPauseBtn,
      frameDisplayEl,
      meta,
      (_id, frame) => {
        setCurrentFrame(frame);
        props.onSplitFrame?.(frame);
      }
    );

    // Autoplay if requested
    if (props.autoplay) {
      setTimeout(() => player.play('autoplay'), 100);
    }
  });

  onCleanup(() => {
    // If the class exposes a cleanup method, call it
    if (typeof (player as any).cleanup === 'function') {
      (player as any).cleanup();
    }
  });

  return (
    <div class="sprite-player">
      <div
        ref={viewerEl}
        class="sprite-viewer bg-gray-100 border border-dashed border-gray-400 p-4 text-center"
        tabindex="0"
      >
        Loading sprite…
      </div>
      <div class="mt-2 flex items-center space-x-2">
        <button
          ref={playPauseBtn}
          class="btn"
          onClick={() => player.togglePlayback()}
          title="Play / Pause"
        >
          ⏯️
        </button>
        <input
          ref={scrubEl}
          type="range"
          disabled
          class="flex-1"
        />
        <div ref={frameDisplayEl} class="text-sm font-mono">
          Frame: {currentFrame()}
        </div>
      </div>
    </div>
  );
}