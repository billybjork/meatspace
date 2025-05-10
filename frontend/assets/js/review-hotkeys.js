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
// * Press ⇧+Space to toggle “ID-mode” (shows sibling grid and allows
//   merge/group with an arbitrary clip ID).
// ────────────────────────────────────────────────────────────────────

import { SplitManager } from "./sprite-player";

export const ReviewHotkeys = {
  mounted() {
    /* Letter → action mapping */
    this.keyMap  = { a: "approve", s: "skip", d: "archive", f: "merge", g: "group" };

    this.armed    = null;   // currently-armed key (a/s/d/f/g)
    this.btn      = null;   // the <button> DOM node
    this.idBox    = null;   // <input id="target-id">
    this.idMode   = false;  // true when ⇧+Space toggles ID-mode on

    this.actionsEl = document.getElementById("review-actions");

    this._onKeyDown = e => {
      const tag    = (e.target.tagName || "").toLowerCase();
      const typing = ["input","textarea"].includes(tag) || e.target.isContentEditable;
      const k      = e.key.toLowerCase();

      // 0. ⇧+Space toggles ID-mode
      if (e.shiftKey && e.code === "Space" && !typing) {
        e.stopImmediatePropagation();
        e.preventDefault();
        this._toggleIdMode();
        return;
      }

      // Undo – ⌘/Ctrl+Z
      if ((e.metaKey||e.ctrlKey) && k === "z") {
        if (!typing) {
          this.pushEvent("undo", {});
          this._reset();
          e.preventDefault();
        }
        return;
      }

      // 1. First press A/S/D/F/G arms that action
      if (this.keyMap[k] && !this.armed) {
        // ignore auto-repeat so we don’t clear the box while typing digits
        if (e.repeat) { e.preventDefault(); return; }

        this.armed = k;
        this.btn   = document.getElementById(`btn-${this.keyMap[k]}`);
        if (this.btn) this.btn.classList.add("is-armed");

        // show ID box if ID-mode is on and it's merge/group
        const act = this.keyMap[k];
        if (this.idMode && (act==="merge"||act==="group")) {
          this._showTargetBox();
        }
        e.preventDefault();
        return;
      }

      // 2. Enter commits the action
      if (e.key === "Enter" && this.armed) {
        const action  = this.keyMap[this.armed];
        const payload = { action };

        if (this.idMode && this.idBox && this.idBox.value.trim() !== "" &&
            (action==="merge"||action==="group")) {
          payload.target_id = this.idBox.value.trim();
        }

        this.pushEvent("select", payload);

        // sync local ID-mode off after commit
        this.idMode = false;
        if (this.actionsEl) this.actionsEl.dataset.idMode = "false";

        this._reset();
        e.preventDefault();
        return;
      }

      // 3. Enter in split-mode commits split
      if (e.key === "Enter" && SplitManager.splitMode) {
        SplitManager.commit((evt,data)=>this.pushEvent(evt,data));
        e.preventDefault();
      }
    };

    // keyup only resets the button highlight—does NOT hide the ID box
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

  /* Toggle ID-mode (⇧+Space) */
  _toggleIdMode() {
    this.idMode = !this.idMode;
    if (this.actionsEl) {
      this.actionsEl.dataset.idMode = this.idMode;
    }
    this.pushEvent("toggle-id-mode", {});

    // hide the box when leaving ID-mode
    if (!this.idMode && this.idBox) {
      this.idBox.classList.add("hidden");
    }

    // if turning ON while a letter is still armed, show it
    if (this.idMode && this.armed) {
      const act = this.keyMap[this.armed];
      if (act==="merge"||act==="group") {
        this._showTargetBox();
      }
    }
  },

  /* Show & focus the numeric input */
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

  /* Reset button highlight (but leave ID box intact) */
  _reset() {
    if (this.btn) this.btn.classList.remove("is-armed");
    this.armed = this.btn = null;
    // note: we no longer hide or clear the ID box here
  }
};