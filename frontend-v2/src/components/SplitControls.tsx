import { Component, createMemo } from 'solid-js';

export interface SplitControlsProps {
  /** current selected frame index */
  frame: number;
  /** total clip frames */
  totalFrames: number;
  /** clip FPS */
  fps: number;
  /** whether a split is in flight */
  isPending: boolean;
  /** error message (if any) from the last split attempt */
  error?: string;
  /** called when user clicks Confirm */
  onConfirm: () => void;
  /** called when user clicks Cancel */
  onCancel: () => void;
}

const SplitControls: Component<SplitControlsProps> = (props) => {
  // compute validity
  const valid = createMemo(() => {
    const m = 1;
    return (
      props.totalFrames > 2 * m &&
      props.frame >= m &&
      props.frame <= props.totalFrames - m - 1
    );
  });
  const time = createMemo(() => (props.frame / props.fps).toFixed(3));

  return (
    <div
      class="split-controls bg-gray-100 border border-dashed border-gray-400 p-4 my-4 text-center"
      style={{ display: 'block' }}
    >
      <div class="frame-info mb-2">
        Frame <span class="font-mono">{props.frame}</span> / 
        <span class="font-mono">{props.totalFrames - 1}</span> | 
        Time <span class="font-mono">{time()}</span>s
      </div>
      <button
        class="action-btn confirm-split-btn mr-2"
        disabled={!valid() || props.isPending}
        onClick={props.onConfirm}
      >
        Confirm ({props.frame})
      </button>
      <button
        class="action-btn cancel-split-btn"
        disabled={props.isPending}
        onClick={props.onCancel}
      >
        Cancel
      </button>
      <div class="split-feedback mt-2 text-sm" style={{ color: props.error ? '#d9534f' : '#5cb85c' }}>
        {props.error
          ? props.error
          : valid()
          ? 'Valid split point.'
          : props.totalFrames <= 2
          ? 'Clip too short to split.'
          : props.frame < 1
          ? 'Cannot split at frame 0.'
          : `Cannot split at last frame ${props.totalFrames - 1}.`}
      </div>
    </div>
  );
};

export default SplitControls;