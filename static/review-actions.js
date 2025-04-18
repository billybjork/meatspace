/**
 * Makes API calls for clip actions and provides basic UI feedback.
 */

// Helper to show feedback and manage button state
function showFeedback(feedbackDiv, message, isError = false) {
    if (!feedbackDiv) return;
    feedbackDiv.textContent = message;
    feedbackDiv.className = `action-feedback ${isError ? 'error' : 'success'}`;
}

function setProcessingState(clipItem, isProcessing) {
    if (!clipItem) return;
    if (isProcessing) {
        clipItem.classList.add('processing');
    } else {
        clipItem.classList.remove('processing');
    }
}

function setDoneState(clipItem, isDone) {
     if (!clipItem) return;
     if (isDone) {
         clipItem.classList.add('done');
     } else {
         clipItem.classList.remove('done');
     }
}

// --- API Action Handlers ---

async function handleAction(clipId, action, clipItem, feedbackDiv, undoButton) {
    if (!clipItem || !feedbackDiv || !undoButton) return;

    console.log(`Action API: ${action} on Clip ID: ${clipId}`);
    setProcessingState(clipItem, true);
    setDoneState(clipItem, false); // Ensure not marked done initially
    showFeedback(feedbackDiv, 'Processing...');
    undoButton.style.display = 'none'; // Hide undo during processing

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
        if (action === 'approve' || action === 'archive') {
            feedbackMsg += ' Item will disappear on refresh.';
        }
        showFeedback(feedbackDiv, feedbackMsg, false);
        setDoneState(clipItem, true); // Mark as done on success

        // Show undo only if not archived or approved
        if (action !== 'approve' && action !== 'archive') {
            undoButton.style.display = 'inline-block';
             setTimeout(() => {
                 // Check if still 'done' before hiding (might have been undone)
                 if (clipItem.classList.contains('done') && undoButton.style.display !== 'none') {
                    undoButton.style.display = 'none';
                }
              }, 15000); // 15 sec undo window
        }
        return { success: true, result: result };

    } catch (error) {
        console.error(`Action API failed for ${action} on ${clipId}:`, error);
        showFeedback(feedbackDiv, `Error: ${error.message}`, true);
        setDoneState(clipItem, false); // Ensure not marked done on error
        // Optionally allow undo again on failure?
        // undoButton.style.display = 'inline-block';
         return { success: false, error: error };
    } finally {
        setProcessingState(clipItem, false);
    }
}


async function handleUndo(clipId, clipItem, feedbackDiv, undoButton) {
    if (!clipItem || !feedbackDiv || !undoButton) return;

    console.log(`Undo API on Clip ID: ${clipId}`);
    setProcessingState(clipItem, true);
    setDoneState(clipItem, false); // Remove done state during undo
    showFeedback(feedbackDiv, 'Undoing...');
    undoButton.style.display = 'none';

    try {
        const response = await fetch(`/api/clips/${clipId}/undo`, {
            method: 'POST',
            headers: { 'Accept': 'application/json' }
        });
        const result = await response.json();
        if (!response.ok) throw new Error(result.detail || `HTTP error! status: ${response.status}`);

        showFeedback(feedbackDiv, `Success: Reverted to ${result.new_state}. Refresh page.`, false);
        // Don't re-add 'done' class after undo
        // Undo button remains hidden

        return { success: true, result: result };

    } catch (error) {
        console.error(`Undo API failed for ${clipId}:`, error);
        showFeedback(feedbackDiv, `Undo Error: ${error.message}`, true);
        // Show undo button again if undo fails?
         undoButton.style.display = 'inline-block';
         return { success: false, error: error };
    } finally {
        setProcessingState(clipItem, false);
    }
}

async function queueSplit(clipId, frame, clipItem, feedbackDiv, splitControlsElement) {
    if (!clipItem || !feedbackDiv || !splitControlsElement) return;

    console.log(`Split API on Clip ID: ${clipId} at frame: ${frame}`);
    setProcessingState(clipItem, true);
    setDoneState(clipItem, false);
    showFeedback(feedbackDiv, 'Queueing split...'); // Main feedback
    const splitFeedback = splitControlsElement.querySelector('.split-feedback');
    if (splitFeedback) splitFeedback.textContent = 'Queueing split...';


    const payload = { split_request_at_frame: frame };

    try {
        const response = await fetch(`/api/clips/${clipId}/split`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
            body: JSON.stringify(payload)
        });
        const result = await response.json();
        if (!response.ok) throw new Error(result.detail || `HTTP error! status: ${response.status}`);

        showFeedback(feedbackDiv, `Success: Queued for splitting (state: ${result.new_state}). Refresh page.`, false);
        if (splitFeedback) splitFeedback.textContent = ''; // Clear split feedback
        setDoneState(clipItem, true);
        splitControlsElement.style.display = 'none'; // Hide split controls on success

        return { success: true, result: result };

    } catch (error) {
        console.error('Split API failed:', error);
        showFeedback(feedbackDiv, `Split Error: ${error.message}`, true); // Show in main feedback
        if (splitFeedback) splitFeedback.textContent = `Error: ${error.message}`; // Show in split feedback too
        setDoneState(clipItem, false);
         return { success: false, error: error };
    } finally {
        setProcessingState(clipItem, false);
    }
}