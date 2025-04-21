/**
 * Makes API calls for clip actions and provides basic UI feedback.
 * Manages UI states like 'processing' and 'done'.
 */

// --- Helper Functions ---

/**
 * Displays feedback messages in the designated area for a clip item.
 * @param {HTMLElement} feedbackDiv - The DOM element to display feedback in.
 * @param {string} message - The message to display.
 * @param {boolean} [isError=false] - Whether the message represents an error.
 */
function showFeedback(feedbackDiv, message, isError = false) {
    if (!feedbackDiv) return;
    feedbackDiv.textContent = message;
    feedbackDiv.className = `action-feedback ${isError ? 'error' : 'success'}`;
}

/**
 * Adds or removes the 'processing' class to visually indicate activity.
 * Disables interactions via CSS while processing.
 * @param {HTMLElement} clipItem - The main container element for the clip review item.
 * @param {boolean} isProcessing - Whether the item is currently being processed.
 */
function setProcessingState(clipItem, isProcessing) {
    if (!clipItem) return;
    if (isProcessing) {
        clipItem.classList.add('processing');
    } else {
        clipItem.classList.remove('processing');
    }
}

/**
 * Adds or removes the 'done' class to visually indicate completion.
 * Used for styling and potentially hiding the undo button after a delay.
 * @param {HTMLElement} clipItem - The main container element for the clip review item.
 * @param {boolean} isDone - Whether the action on the item is considered done.
 */
function setDoneState(clipItem, isDone) {
     if (!clipItem) return;
     if (isDone) {
         clipItem.classList.add('done');
     } else {
         clipItem.classList.remove('done');
     }
}

// --- API Action Handlers ---

/**
 * Handles standard clip actions like approve, skip, archive, merge, retry.
 * Makes ALL successful actions undoable via the UI button.
 * @param {string} clipId - The database ID of the clip.
 * @param {string} action - The action being performed (e.g., 'skip', 'archive').
 * @param {HTMLElement} clipItem - The clip item's container element.
 * @param {HTMLElement} feedbackDiv - The element for displaying feedback messages.
 * @param {HTMLElement} undoButton - The undo button element for this clip.
 * @param {function(string|null)} setLastUndoableClipId - Function to update the globally tracked undoable clip ID.
 * @returns {Promise<{success: boolean, result?: any, error?: Error}>} - Promise resolving with action result.
 */
async function handleAction(clipId, action, clipItem, feedbackDiv, undoButton, setLastUndoableClipId) {
    // Validate essential arguments
    if (!clipId || !action || !clipItem || !feedbackDiv || !undoButton || typeof setLastUndoableClipId !== 'function') {
         console.error("handleAction called with missing or invalid arguments.", { clipId, action, clipItem, feedbackDiv, undoButton, setLastUndoableClipId });
         if(feedbackDiv) showFeedback(feedbackDiv, "Internal error: Missing arguments for action.", true);
         return { success: false, error: new Error("Missing arguments for handleAction") };
    }

    setProcessingState(clipItem, true);
    setDoneState(clipItem, false);
    showFeedback(feedbackDiv, 'Processing...');
    undoButton.style.display = 'none'; // Ensure hidden initially
    setLastUndoableClipId(null); // Clear previous undo target

    const payload = { action: action };

    try {
        const response = await fetch(`/api/clips/${clipId}/action`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
            body: JSON.stringify(payload)
        });
        const result = await response.json();
        if (!response.ok) throw new Error(result.detail || `HTTP error! status: ${response.status}`);

        let feedbackMsg = `Success: Marked as ${result.new_state}.`;
        showFeedback(feedbackDiv, feedbackMsg, false);
        setDoneState(clipItem, true); // Mark as done on success

        undoButton.style.display = 'inline-block'; // Attempt to show
        setLastUndoableClipId(clipId); // Track this clip for potential Ctrl+Z undo

        // Set timeout to hide the undo button after a delay
        setTimeout(() => {
            const currentClipItem = document.getElementById(`clip-${clipId}`);
            if (currentClipItem && currentClipItem.classList.contains('done')) {
                 const currentUndoButton = currentClipItem.querySelector('.undo-button');
                 // Check computed style for visibility
                 if (currentUndoButton && window.getComputedStyle(currentUndoButton).display !== 'none') {
                      currentUndoButton.style.display = 'none'; // Hide it again
                 }
            }
         }, 15000); // 15 second undo window

        return { success: true, result: result };

    } catch (error) {
        console.error(`Action API failed for ${action} on ${clipId}:`, error);
        showFeedback(feedbackDiv, `Error: ${error.message}`, true);
        setDoneState(clipItem, false);
        return { success: false, error: error };
    } finally {
        setProcessingState(clipItem, false);
    }
}

/**
 * Handles the Undo action for a clip.
 * @param {string} clipId - The database ID of the clip to undo.
 * @param {HTMLElement} clipItem - The clip item's container element.
 * @param {HTMLElement} feedbackDiv - The element for displaying feedback messages.
 * @param {HTMLElement} undoButton - The undo button element for this clip.
 * @param {function(string|null)} setLastUndoableClipId - Function to update the globally tracked undoable clip ID.
 * @returns {Promise<{success: boolean, result?: any, error?: Error}>} - Promise resolving with undo result.
 */
async function handleUndo(clipId, clipItem, feedbackDiv, undoButton, setLastUndoableClipId) {
     // Validate essential arguments
    if (!clipId || !clipItem || !feedbackDiv || !undoButton || typeof setLastUndoableClipId !== 'function') {
         console.error("handleUndo called with missing or invalid arguments.", { clipId, clipItem, feedbackDiv, undoButton, setLastUndoableClipId });
          if(feedbackDiv) showFeedback(feedbackDiv, "Internal error: Missing arguments for undo.", true);
         return { success: false, error: new Error("Missing arguments for handleUndo") };
    }

    setProcessingState(clipItem, true);
    setDoneState(clipItem, false);
    showFeedback(feedbackDiv, 'Undoing...');
    undoButton.style.display = 'none'; // Hide undo button immediately
    setLastUndoableClipId(null); // Clear the global undo target

    try {
        const response = await fetch(`/api/clips/${clipId}/undo`, {
            method: 'POST',
            headers: { 'Accept': 'application/json' }
        });
        const result = await response.json();
        if (!response.ok) throw new Error(result.detail || `HTTP error! status: ${response.status}`);

        showFeedback(feedbackDiv, `Success: Reverted to ${result.new_state}. Refresh page recommended.`, false);
        // Undo button remains hidden.

        return { success: true, result: result };

    } catch (error) {
        console.error(`Undo API failed for ${clipId}:`, error);
        showFeedback(feedbackDiv, `Undo Error: ${error.message}`, true);
        return { success: false, error: error };
    } finally {
        setProcessingState(clipItem, false);
    }
}

/**
 * Handles the action of queueing a clip for splitting. (Already treated as undoable)
 * @param {string} clipId - The database ID of the clip.
 * @param {number} frame - The frame number at which to split the clip.
 * @param {HTMLElement} clipItem - The clip item's container element.
 * @param {HTMLElement} feedbackDiv - The main feedback element for the clip item.
 * @param {HTMLElement} splitControlsElement - The container for the split controls UI.
 * @param {HTMLElement} undoButton - The undo button element for this clip.
 * @param {function(string|null)} setLastUndoableClipId - Function to update the globally tracked undoable clip ID.
 * @returns {Promise<{success: boolean, result?: any, error?: Error}>} - Promise resolving with split queue result.
 */
async function queueSplit(clipId, frame, clipItem, feedbackDiv, splitControlsElement, undoButton, setLastUndoableClipId) {
    // Validate essential arguments
    if (!clipId || typeof frame !== 'number' || !clipItem || !feedbackDiv || !splitControlsElement || !undoButton || typeof setLastUndoableClipId !== 'function') {
        console.error("queueSplit called with missing or invalid arguments.", { clipId, frame, clipItem, feedbackDiv, splitControlsElement, undoButton, setLastUndoableClipId });
        if(feedbackDiv) showFeedback(feedbackDiv, "Internal error: Missing arguments for split.", true);
         return { success: false, error: new Error("Missing arguments for queueSplit") };
    }

    setProcessingState(clipItem, true);
    setDoneState(clipItem, false);
    showFeedback(feedbackDiv, 'Queueing split...');
    const splitFeedback = splitControlsElement.querySelector('.split-feedback');
    if (splitFeedback) splitFeedback.textContent = 'Queueing split...';
    undoButton.style.display = 'none'; // Ensure hidden initially
    setLastUndoableClipId(null); // Clear previous undo target

    const payload = { split_request_at_frame: frame };

    try {
        const response = await fetch(`/api/clips/${clipId}/split`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
            body: JSON.stringify(payload)
        });
        const result = await response.json();
        if (!response.ok) throw new Error(result.detail || `HTTP error! status: ${response.status}`);

        showFeedback(feedbackDiv, `Success: Queued for splitting (state: ${result.new_state}). Refresh recommended.`, false);
        if (splitFeedback) splitFeedback.textContent = '';
        setDoneState(clipItem, true);
        splitControlsElement.style.display = 'none';

        // --- Undo Logic for Split ---
        undoButton.style.display = 'inline-block'; // Attempt to show
        setLastUndoableClipId(clipId); // Track this clip for potential Ctrl+Z undo

        // Set timeout to hide the undo button after a delay
         setTimeout(() => {
             const currentClipItem = document.getElementById(`clip-${clipId}`);
             if (currentClipItem && currentClipItem.classList.contains('done')) {
                 const currentUndoButton = currentClipItem.querySelector('.undo-button');
                 // Check computed style for visibility
                 if (currentUndoButton && window.getComputedStyle(currentUndoButton).display !== 'none') {
                      currentUndoButton.style.display = 'none'; // Hide it again
                 }
             }
         }, 15000); // 15 second undo window

        return { success: true, result: result };

    } catch (error) {
        console.error(`Split API failed for clip ${clipId}:`, error);
        showFeedback(feedbackDiv, `Split Error: ${error.message}`, true);
        if (splitFeedback) {
             splitFeedback.textContent = `Error: ${error.message}`;
             splitFeedback.style.color = '#d00';
         }
        setDoneState(clipItem, false);
         return { success: false, error: error };
    } finally {
        setProcessingState(clipItem, false);
    }
}