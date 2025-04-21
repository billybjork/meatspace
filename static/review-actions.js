/**
 * Displays a temporary feedback message.
 * @param {HTMLElement} feedbackDiv - The DOM element to display feedback in.
 * @param {string} message - The message to display.
 * @param {boolean} [isError=false] - Whether the message represents an error.
 */
function showFeedback(feedbackDiv, message, isError = false) {
    if (!feedbackDiv) return;
    feedbackDiv.textContent = message;
    feedbackDiv.className = `action-feedback ${isError ? 'error' : 'success'}`;
    // Clear feedback after a short delay if not reloading immediately
    // setTimeout(() => { if(feedbackDiv) feedbackDiv.textContent = ''; }, isError ? 5000 : 2000);
}

/**
 * Sets a processing state (visual only, no pointer events needed if reload happens).
 * @param {HTMLElement} clipItem - The main container element for the clip review item.
 * @param {boolean} isProcessing - Whether the item is currently being processed.
 */
function setProcessingState(clipItem, isProcessing) {
    if (!clipItem) return;
    if (isProcessing) {
        clipItem.classList.add('processing'); // Simple visual cue
    } else {
        clipItem.classList.remove('processing');
    }
}

/**
 * Makes the API call for standard actions (approve, skip, archive, merge_previous, retry).
 * Reloads the page on success.
 * @param {string} clipId - The database ID of the clip.
 * @param {string} action - The action being performed.
 * @param {HTMLElement} clipItem - The clip item's container element.
 * @param {HTMLElement} feedbackDiv - The element for displaying feedback messages.
 * @returns {Promise<{success: boolean, error?: Error}>} Resolves/rejects based on API call.
 */
async function performApiAction(clipId, action, clipItem, feedbackDiv) {
    if (!clipId || !action || !clipItem || !feedbackDiv) {
         console.error("performApiAction called with missing arguments.");
         if(feedbackDiv) showFeedback(feedbackDiv, "Internal UI error.", true);
         return { success: false, error: new Error("Missing arguments") };
    }

    setProcessingState(clipItem, true);
    showFeedback(feedbackDiv, `Processing ${action}...`);

    const payload = { action: action };

    try {
        const response = await fetch(`/api/clips/${clipId}/action`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
            body: JSON.stringify(payload)
        });
        const result = await response.json(); // Try to parse JSON even on error for details

        if (!response.ok) {
            // Use detail from JSON if available, otherwise use status text
            const errorDetail = result?.detail || `HTTP error ${response.status}`;
            throw new Error(errorDetail);
        }

        showFeedback(feedbackDiv, `Success: ${action} processed. Loading next...`, false);
        // Reload the page to get the next clip
        window.location.reload();
        // Return success, although the reload interrupts further JS execution here
        return { success: true };

    } catch (error) {
        console.error(`API action '${action}' failed for ${clipId}:`, error);
        showFeedback(feedbackDiv, `Error (${action}): ${error.message}`, true);
        setProcessingState(clipItem, false); // Re-enable interaction on error
        return { success: false, error: error };
    }
    // No finally needed for processing state if success causes reload
}

/**
 * Makes the API call to queue a split action.
 * Reloads the page on success.
 * @param {string} clipId - The database ID of the clip.
 * @param {number} frame - The frame number at which to split the clip.
 * @param {HTMLElement} clipItem - The clip item's container element.
 * @param {HTMLElement} feedbackDiv - The main feedback element for the clip item.
 * @param {HTMLElement} splitControlsElement - The container for the split controls UI.
 * @returns {Promise<{success: boolean, error?: Error}>} Resolves/rejects based on API call.
 */
async function performApiSplit(clipId, frame, clipItem, feedbackDiv, splitControlsElement) {
     if (!clipId || typeof frame !== 'number' || !clipItem || !feedbackDiv || !splitControlsElement) {
        console.error("performApiSplit called with missing arguments.");
        if(feedbackDiv) showFeedback(feedbackDiv, "Internal UI error.", true);
        return { success: false, error: new Error("Missing arguments") };
    }

    setProcessingState(clipItem, true);
    showFeedback(feedbackDiv, 'Queueing split...');
    const splitFeedback = splitControlsElement.querySelector('.split-feedback');
    if (splitFeedback) splitFeedback.textContent = 'Queueing split...';

    const payload = { split_request_at_frame: frame };

    try {
        const response = await fetch(`/api/clips/${clipId}/split`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
            body: JSON.stringify(payload)
        });
        const result = await response.json(); // Try parsing on error too

        if (!response.ok) {
             const errorDetail = result?.detail || `HTTP error ${response.status}`;
             throw new Error(errorDetail);
        }

        showFeedback(feedbackDiv, 'Success: Split queued. Loading next...', false);
        // Reload the page
        window.location.reload();
        return { success: true };

    } catch (error) {
        console.error(`Split API failed for clip ${clipId}:`, error);
        const errorMsg = `Split Error: ${error.message}`;
        showFeedback(feedbackDiv, errorMsg, true);
        if (splitFeedback) {
             splitFeedback.textContent = errorMsg;
             splitFeedback.style.color = '#d00';
         }
        setProcessingState(clipItem, false); // Re-enable on error
        return { success: false, error: error };
    }
}