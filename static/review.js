document.addEventListener('DOMContentLoaded', () => {
    const reviewQueue = document.getElementById('review-queue');

    reviewQueue.addEventListener('click', async (event) => {
        const target = event.target;
        const clipItem = target.closest('.clip-review-item');
        if (!clipItem) return;

        const clipId = clipItem.dataset.clipId;
        const feedbackDiv = clipItem.querySelector('.action-feedback');
        const undoButton = clipItem.querySelector('.undo-button');
        const videoPlayer = clipItem.querySelector('video');
        const splitControls = clipItem.querySelector('.split-controls');
        const splitModeBtn = clipItem.querySelector('.split-mode-btn');
        const confirmSplitBtn = clipItem.querySelector('.confirm-split-btn');
        const cancelSplitBtn = clipItem.querySelector('.cancel-split-btn');
        const splitCurrentTimeSpan = clipItem.querySelector('.split-current-time');
        const splitConfirmTimeSpan = clipItem.querySelector('.split-confirm-time');
        const splitFeedbackSpan = clipItem.querySelector('.split-feedback');

        let action = null;
        let payload = {};

        // --- Handle Split Mode Activation ---
        if (target.matches('.split-mode-btn')) {
            action = target.dataset.action;
            console.log(`Action: ${action} on Clip ID: ${clipId}`);
            splitControls.style.display = 'block';
            splitModeBtn.style.display = 'none'; // Hide the initial split button
            splitFeedbackSpan.textContent = ''; // Clear feedback
            if (videoPlayer) {
                videoPlayer.pause(); // Pause for easier selection
                // Function to update time display and enable confirm button
                const updateSplitTime = () => {
                    const currentTime = videoPlayer.currentTime;
                    splitCurrentTimeSpan.textContent = currentTime.toFixed(3); // More precision?
                    splitConfirmTimeSpan.textContent = currentTime.toFixed(3); // More precision?

                    const duration = videoPlayer.duration;
                    const minSplitMargin = 0.1; // Reduced margin (e.g., 100ms)

                    let isDisabled = true; // Default to disabled
                    let feedbackMsg = '';

                    if (isNaN(duration) || duration <= 0) {
                        feedbackMsg = 'Video duration unknown.';
                    } else if (currentTime <= minSplitMargin) {
                        feedbackMsg = `Select time > ${minSplitMargin}s from start.`;
                    } else if (currentTime >= (duration - minSplitMargin)) {
                        feedbackMsg = `Select time < ${minSplitMargin}s from end.`;
                    } else {
                        isDisabled = false; // Enable if checks pass
                    }

                    confirmSplitBtn.disabled = isDisabled;
                    splitFeedbackSpan.textContent = feedbackMsg;
                };
                // Attach listener
                videoPlayer.addEventListener('timeupdate', updateSplitTime);
                videoPlayer.addEventListener('seeked', updateSplitTime); // Also update on seek
                // Store listener reference to remove it later
                clipItem.timeUpdateListener = updateSplitTime;
                updateSplitTime(); // Initial update
            }
            return; // Don't proceed with other actions
        }

        // --- Handle Split Mode Cancellation ---
        if (target.matches('.cancel-split-btn')) {
            action = target.dataset.action;
            console.log(`Action: ${action} on Clip ID: ${clipId}`);
            splitControls.style.display = 'none';
            splitModeBtn.style.display = 'inline-block'; // Show initial button again
            if (videoPlayer && clipItem.timeUpdateListener) {
                videoPlayer.removeEventListener('timeupdate', clipItem.timeUpdateListener);
                videoPlayer.removeEventListener('seeked', clipItem.timeUpdateListener); // Remove seek listener too
                delete clipItem.timeUpdateListener; // Clean up
            }
            return; // Don't proceed
        }

        // --- Handle Split Confirmation ---
        if (target.matches('.confirm-split-btn')) {
            action = target.dataset.action;
            const splitTime = parseFloat(splitConfirmTimeSpan.textContent);
            console.log(`Action: ${action} on Clip ID: ${clipId} at time: ${splitTime}s`);

            if (isNaN(splitTime) || splitTime <= 0) {
                 splitFeedbackSpan.textContent = 'Invalid split time.';
                 return;
            }

            payload = { split_time_seconds: splitTime }; // Payload for the split action

            clipItem.classList.add('processing');
            splitFeedbackSpan.textContent = 'Queueing split...';
            feedbackDiv.textContent = ''; // Clear main feedback

            try {
                const response = await fetch(`/api/clips/${clipId}/split`, { // NEW ENDPOINT
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Accept': 'application/json'
                    },
                    body: JSON.stringify(payload)
                });
                const result = await response.json();
                if (!response.ok) throw new Error(result.detail || `HTTP error! status: ${response.status}`);

                feedbackDiv.textContent = `Success: Queued for splitting (state: ${result.new_state}). This item will disappear once processed.`;
                feedbackDiv.className = 'action-feedback success';
                clipItem.classList.remove('processing');
                clipItem.classList.add('done'); // Mark as done visually
                splitControls.style.display = 'none'; // Hide split controls

            } catch (error) {
                console.error('Split action failed:', error);
                splitFeedbackSpan.textContent = `Error: ${error.message}`;
                feedbackDiv.textContent = `Split Error: ${error.message}`; // Also show in main feedback
                feedbackDiv.className = 'action-feedback error';
                clipItem.classList.remove('processing');
            } finally {
                 // Clean up listener even if API call fails
                 if (videoPlayer && clipItem.timeUpdateListener) {
                     videoPlayer.removeEventListener('timeupdate', clipItem.timeUpdateListener);
                     videoPlayer.removeEventListener('seeked', clipItem.timeUpdateListener);
                     delete clipItem.timeUpdateListener;
                 }
            }
            return; // Stop further processing
        }

        // --- Handle standard action buttons (Approve, Skip, Archive, Merge Next) ---
        if (target.matches('.action-btn') && !target.matches('.split-mode-btn') && !target.matches('.confirm-split-btn') && !target.matches('.cancel-split-btn')) {
            action = target.dataset.action;
            payload.action = action;

            console.log(`Action: ${action} on Clip ID: ${clipId}`);

            clipItem.classList.add('processing');
            feedbackDiv.textContent = 'Processing...';
            feedbackDiv.className = 'action-feedback';
            undoButton.style.display = 'none';

            try {
                const response = await fetch(`/api/clips/${clipId}/action`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
                    body: JSON.stringify(payload)
                });
                const result = await response.json();
                if (!response.ok) throw new Error(result.detail || `HTTP error! status: ${response.status}`);

                feedbackDiv.textContent = `Success: Marked as ${result.new_state}.`;
                feedbackDiv.className = 'action-feedback success';
                clipItem.classList.remove('processing');
                clipItem.classList.add('done');
                undoButton.style.display = 'inline-block';
                setTimeout(() => {
                    if (clipItem.classList.contains('done')) { // Check if still 'done' before hiding
                        undoButton.style.display = 'none';
                    }
                 }, 15000);

            } catch (error) {
                console.error('Action failed:', error);
                feedbackDiv.textContent = `Error: ${error.message}`;
                feedbackDiv.className = 'action-feedback error';
                clipItem.classList.remove('processing');
            }

        } else if (target.matches('.undo-button')) {
            // --- Keep Existing Undo Logic ---
            console.log(`Undo Action on Clip ID: ${clipId}`);
            clipItem.classList.add('processing');
            feedbackDiv.textContent = 'Undoing...';
            feedbackDiv.className = 'action-feedback';
            undoButton.style.display = 'none';

            try {
                const response = await fetch(`/api/clips/${clipId}/undo`, {
                    method: 'POST',
                    headers: { 'Accept': 'application/json' }
                });
                const result = await response.json();
                if (!response.ok) throw new Error(result.detail || `HTTP error! status: ${response.status}`);

                feedbackDiv.textContent = `Success: Reverted to ${result.new_state}. Refresh to see it back in queue.`;
                feedbackDiv.className = 'action-feedback success';
                clipItem.classList.remove('processing', 'done');
                // No need to show undo again after successful undo

            } catch (error) {
                console.error('Undo failed:', error);
                feedbackDiv.textContent = `Undo Error: ${error.message}`;
                feedbackDiv.className = 'action-feedback error';
                clipItem.classList.remove('processing');
                // Optionally show undo button again if undo fails, or leave error.
                 undoButton.style.display = 'inline-block'; // Show undo again if it failed
            }
        }
    });
});