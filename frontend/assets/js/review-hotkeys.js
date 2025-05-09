// Phoenix LiveView hook that lets reviewers work the whole queue
// from the keyboard:
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
// Hold the letter → button highlights → <ENTER> commits.
// While split-mode is armed, <ENTER> also commits the split.
// Press Ctrl/Cmd+Z at any time to undo the last action.

import { SplitManager } from "./sprite-player";

export const ReviewHotkeys = {
  mounted() {
    /*  A ➜ approve, …                                                    */
    this.keyMap  = { a: "approve", s: "skip", d: "archive",
                     f: "merge",   g: "group" };
    this.armed   = null;               // e.g. "a"
    this.btn     = null;               // DOM node of highlighted button

    /* —————————————————————————— listeners ———————————————————————————— */
    this._onKeyDown = e => {
      // Ctrl/Cmd+Z → undo (unless typing in input/textarea/contentEditable)
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "z") {
        const tag = (e.target.tagName || "").toLowerCase();
        const typing = ["input", "textarea"].includes(tag) || e.target.isContentEditable;
        if (!typing) {
          this.pushEvent("undo", {});
          this._reset();
          e.preventDefault();  // prevent browser undo
        }
        return;
      }

      const k = e.key.toLowerCase();

      /* 1.  First press of A/S/D/F/G – arm + highlight */
      if (this.keyMap[k] && !this.armed) {
        this.armed = k;
        this.btn   = document.getElementById(`btn-${this.keyMap[k]}`);
        if (this.btn) this.btn.classList.add("is-armed");
        e.preventDefault();
        return;
      }

      /* 2.  <ENTER> while armed – commit select action */
      if (e.key === "Enter" && this.armed) {
        const action = this.keyMap[this.armed];
        this.pushEvent("select", { action });
        this._reset();
        e.preventDefault();
        return;
      }

      /* 3.  <ENTER> while split-mode armed – commit split */
      if (e.key === "Enter" && SplitManager.splitMode) {
        SplitManager.commit((evt, payload) =>
          this.pushEvent(evt, payload));
        e.preventDefault();
      }
    };

    this._onKeyUp = e => {
      if (e.key.toLowerCase() === this.armed) this._reset();
    };

    window.addEventListener("keydown", this._onKeyDown);
    window.addEventListener("keyup",   this._onKeyUp);
  },

  destroyed() {
    window.removeEventListener("keydown", this._onKeyDown);
    window.removeEventListener("keyup",   this._onKeyUp);
  },

  /* -------------------------------------------------------------- */
  _reset() {
    if (this.btn) this.btn.classList.remove("is-armed");
    this.armed = this.btn = null;
  }
};