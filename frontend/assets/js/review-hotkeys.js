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
// ────────────────────────────────────────────────────────────────────
// * Hold a letter → the corresponding button highlights (is-armed)
//
// * Press <ENTER> while a letter is armed → commits the action.
//   - If “ID-mode” (⌘/Ctrl + Space) is ON *and* the action is merge/group,
//     the little numeric input becomes visible; whatever you type is sent
//     as `target_id`.
//
// * Split-mode is handled in sprite-player.js ; <ENTER> commits a split.
//
// * Press ⌘/Ctrl+Z anywhere to undo the last action.
//
// * Press ⌘/Shift+Space to toggle “ID-mode” (shows sibling grid and allows
//   merge/group with an arbitrary clip ID).
// ────────────────────────────────────────────────────────────────────
import { SplitManager } from "./sprite-player";

export const ReviewHotkeys = {
  mounted() {
    /* Letter → action mapping ----------------------------------------- */
    this.keyMap  = { a: "approve", s: "skip", d: "archive",
                     f: "merge",   g: "group" };

    this.armed    = null;   // e.g. "a"
    this.btn      = null;   // DOM node of highlighted button
    this.idBox    = null;   // <input id="target-id">
    this.idMode   = false;  // toggled by ⌘/Ctrl+Space

    /* Element whose [data-id-mode] gets flipped for CSS                 */
    this.actionsEl = document.getElementById("review-actions");

    /* ——————————————— listeners ——————————————— */
    this._onKeyDown = e => {
      const tag    = (e.target.tagName || "").toLowerCase();
      const typing = ["input", "textarea"].includes(tag) || e.target.isContentEditable;
      const k      = e.key.toLowerCase();

      /* 0.  Shift + Space → toggle ID-mode  --------------------------- */
      if (e.shiftKey && e.code === "Space" && !typing) {
        e.stopImmediatePropagation();   // ← prevent SpritePlayer’s handler
        e.preventDefault();             // ← prevent page scrolling, etc.
        this._toggleIdMode();
        e.preventDefault();
        return;
      }

      /* Undo – ⌘/Ctrl+Z (unless typing) ------------------------------ */
      if ((e.metaKey || e.ctrlKey) && k === "z") {
        if (!typing) {
          this.pushEvent("undo", {});
          this._reset();
          e.preventDefault();           // stop browser “undo”
        }
        return;
      }

      /* 1.  First press of A/S/D/F/G – arm & highlight --------------- */
      if (this.keyMap[k] && !this.armed) {
        this.armed = k;
        this.btn   = document.getElementById(`btn-${this.keyMap[k]}`);
        if (this.btn) this.btn.classList.add("is-armed");

        /* Show numeric box if ID-mode ON and action needs target id     */
        const act = this.keyMap[k];
        if (this.idMode && (act === "merge" || act === "group")) {
          this._showTargetBox();
        }
        e.preventDefault();
        return;
      }

      /* 2.  <ENTER> while a letter is armed – commit select ---------- */
      if (e.key === "Enter" && this.armed) {
        const action  = this.keyMap[this.armed];
        const payload = { action };

        if (this.idMode && this.idBox && this.idBox.value.trim() !== "" &&
            (action === "merge" || action === "group")) {
          payload["target_id"] = this.idBox.value.trim();
        }

        this.pushEvent("select", payload);
        this._reset();
        e.preventDefault();
        return;
      }

      /* 3.  <ENTER> while split-mode armed – commit split ------------ */
      if (e.key === "Enter" && SplitManager.splitMode) {
        SplitManager.commit((evt, data) => this.pushEvent(evt, data));
        e.preventDefault();
      }
    };

    this._onKeyUp = e => {
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

  /* ------------------------------------------------------------------ */
  _toggleIdMode() {
    this.idMode = !this.idMode;

    /* reflect in DOM for Tailwind/CSS `[data-id-mode]` hooks          */
    if (this.actionsEl) {
      this.actionsEl.dataset.idMode = this.idMode;
    }

    /* inform LiveView – it will add/remove sibling grid               */
    this.pushEvent("toggle-id-mode", {});

    /* if we just *left* ID-mode, hide the box immediately             */
    if (!this.idMode && this.idBox) {
      this.idBox.classList.add("hidden");
    }
  },

  _showTargetBox() {
    if (!this.idBox) {
      this.idBox = document.getElementById("target-id");
    }
    if (this.idBox) {
      this.idBox.classList.remove("hidden");
      this.idBox.focus();
      this.idBox.select();
    }
  },

  _reset() {
    /* button highlight ----------------------------------------------- */
    if (this.btn) this.btn.classList.remove("is-armed");
    this.armed = this.btn = null;

    /* target-id box --------------------------------------------------- */
    if (this.idBox) {
      this.idBox.classList.add("hidden");
      this.idBox.value = "";
    }
  }
};