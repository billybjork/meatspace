// Phoenix LiveView hook that lets reviewers work the whole queue
// entirely from the keyboard.
//
//   ┌─────────────┬────────┐
//   │ Letter-key  │ Action │
//   ├─────────────┼────────┤
//   │  A          │ approve│
//   │  S          │ skip   │
//   │  D          │ archive│
//   │  F          │ merge  │
//   │  G          │ group  │
//   └─────────────┴────────┘
//
// Overview:
//  * Hold a letter → the corresponding button highlights (is-armed).
//  * Press ENTER while a letter is armed → commits the action.
//      - If ID-mode (⇧+Space) is ON and the action is merge or group,
//        the value in the numeric input is sent as `target_id`.
//  * Split-mode is handled in sprite-player.js; ENTER commits a split.
//  * Press ⌘/Ctrl+Z to undo the last action.
//  * Press ⇧+Space to toggle ID-mode (shows sibling grid + ID box).
//  * While in ID-mode, ⇧+←/→ paginate the sibling grid (unless in split-mode).

import { SplitManager } from "./sprite-player";

export const ReviewHotkeys = {
  mounted() {
    // Map single-letter keys to their respective actions
    this.keyMap    = { a: "approve", s: "skip", d: "archive", f: "merge", g: "group" };
    this.armed     = null;             // currently-armed key, e.g. "f"
    this.btn       = null;             // highlighted button element
    this.idMode    = false;            // true when ID-mode is active
    this.idBox     = document.getElementById("target-id");
    this.actionsEl = document.getElementById("review-actions");

    // Key-down handler: manages arming keys, toggling modes, and committing actions
    this._onKeyDown = (e) => {
      const tag  = (e.target.tagName || "").toLowerCase();
      const inId = tag === "input" && e.target.id === "target-id";
      const k    = e.key.toLowerCase();

      // Let digits go into the ID-input uninterrupted, but still handle
      // all other keys (so our hotkeys work even when it's focused).
      if (inId && /\d/.test(k)) {
        return;
      }

      // ─────────────── Pagination in ID-mode ───────────────
      // Shift+ArrowRight/Left pages the sibling grid, but only when not splitting
      if (this.idMode && e.shiftKey && !e.ctrlKey && !e.metaKey) {
        // If we’re in split-mode, let split‐manager have the arrows
        if (SplitManager.splitMode) {
          return;
        }

        const sibSection = document.getElementById("siblings");
        if (sibSection) {
          const page = parseInt(sibSection.dataset.siblingPage, 10);

          if (e.key === "ArrowRight") {
            const nextBtn = document.querySelector(
              `#siblings .pagination button[phx-value-page="${page + 1}"]`
            );
            if (nextBtn) {
              e.preventDefault();
              e.stopImmediatePropagation();
              nextBtn.click();
            }
            return;
          }

          if (e.key === "ArrowLeft") {
            const prevBtn = document.querySelector(
              `#siblings .pagination button[phx-value-page="${page - 1}"]`
            );
            if (prevBtn) {
              e.preventDefault();
              e.stopImmediatePropagation();
              prevBtn.click();
            }
            return;
          }
        }
      }
      // ────────────────────────────────────────────────────────

      // ⇧+Space toggles ID-mode
      if (e.shiftKey && e.code === "Space" && !inId) {
        e.preventDefault();
        this._toggleIdMode();
        return;
      }

      // Undo – ⌘/Ctrl+Z
      if ((e.metaKey || e.ctrlKey) && k === "z") {
        if (!inId) {
          this.pushEvent("undo", {});
          this._reset();
          e.preventDefault();
        }
        return;
      }

      // 1) First press of A/S/D/F/G → arm and highlight
      if (this.keyMap[k] && !this.armed) {
        if (e.repeat) { e.preventDefault(); return; }
        this.armed = k;
        this.btn   = document.getElementById(`btn-${this.keyMap[k]}`);
        this.btn?.classList.add("is-armed");
        e.preventDefault();
        return;
      }

      // 2) ENTER commits either split or the armed action
      if (e.key === "Enter") {
        // Split-mode has priority
        if (SplitManager.splitMode) {
          SplitManager.commit((evt, data) => this.pushEvent(evt, data));
          e.preventDefault();
          return;
        }
        // If a letter is armed, commit that action
        if (this.armed) {
          const action  = this.keyMap[this.armed];
          const payload = { action };

          // If ID-mode is on and merge/group, include target_id
          if (
            this.idMode &&
            this.idBox &&
            this.idBox.value.trim() !== "" &&
            (action === "merge" || action === "group")
          ) {
            payload.target_id = this.idBox.value.trim();
          }

          this.pushEvent("select", payload);

          // Exit ID-mode after committing a merge/group with ID
          this.idMode = false;
          if (this.actionsEl) this.actionsEl.dataset.idMode = "false";
          this.idBox.blur();

          this._reset();
          e.preventDefault();
        }
      }
    };

    // Key-up handler: clears the button highlight
    this._onKeyUp = (e) => {
      if (e.key.toLowerCase() === this.armed) {
        this._reset();
      }
    };

    window.addEventListener("keydown", this._onKeyDown);
    window.addEventListener("keyup",   this._onKeyUp);
  },

  destroyed() {
    window.removeEventListener("keydown", this._onKeyDown);
    window.removeEventListener("keyup",   this._onKeyUp);
  },

  // Toggle ID-mode on/off
  _toggleIdMode() {
    this.idMode = !this.idMode;
    if (this.actionsEl) this.actionsEl.dataset.idMode = this.idMode;
    this.pushEvent("toggle-id-mode", {});
    if (this.idMode) {
      this.idBox.focus();
      this.idBox.select();
    }
  },

  // Reset armed state and button highlight (ID box remains as-is)
  _reset() {
    this.btn?.classList.remove("is-armed");
    this.armed = this.btn = null;
  }
};