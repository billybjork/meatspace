document.addEventListener('DOMContentLoaded', () => {
    const reviewQueue = document.getElementById('review-queue');

    reviewQueue.addEventListener('click', async (event) => {
        const target = event.target;
        const clipItem = target.closest('.clip-review-item');
        if (!clipItem) return;

        const clipId = clipItem.dataset.clipId;
        const feedbackDiv = clipItem.querySelector('.action-feedback');
        const undoButton = clipItem.querySelector('.undo-button');
        let action = null;
        let payload = {};

        if (target.matches('.action-btn')) {
            action = target.dataset.action;
            payload.action = action;

            if (action === 'split') {
                const timeInput = clipItem.querySelector('.split-time-input');
                const splitTime = parseFloat(timeInput.value);
                if (isNaN(splitTime) || splitTime <= 0) {
                    feedbackDiv.textContent = 'Error: Please enter a valid positive split time.';
                    feedbackDiv.className = 'action-feedback error';
                    return; // Don't proceed
                }
                payload.split_at_seconds = splitTime;
            }
            console.log(`Action: ${action} on Clip ID: ${clipId}`, payload); // Debug log
            clipItem.classList.add('processing'); // Visual feedback
            feedbackDiv.textContent = 'Processing...';
            feedbackDiv.className = 'action-feedback';
            undoButton.style.display = 'none'; // Hide undo during processing

            try {
                const response = await fetch(`/api/clips/${clipId}/action`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Accept': 'application/json'
                    },
                    body: JSON.stringify(payload)
                });

                const result = await response.json(); // Always try to parse JSON

                if (!response.ok) {
                    throw new Error(result.detail || `HTTP error! status: ${response.status}`);
                }

                feedbackDiv.textContent = `Success: Marked as ${result.new_state}.`;
                feedbackDiv.className = 'action-feedback success';
                clipItem.classList.remove('processing');
                clipItem.classList.add('done'); // Mark as done visually
                // Optionally hide the item after a delay:
                // setTimeout(() => clipItem.style.display = 'none', 2000);

                // Show Undo button temporarily
                undoButton.style.display = 'inline-block';
                setTimeout(() => {
                    // Hide undo only if the item hasn't been reset by an undo action
                    if (clipItem.classList.contains('done')) {
                        undoButton.style.display = 'none';
                    }
                }, 15000); // 15 seconds undo window


            } catch (error) {
                console.error('Action failed:', error);
                feedbackDiv.textContent = `Error: ${error.message}`;
                feedbackDiv.className = 'action-feedback error';
                clipItem.classList.remove('processing'); // Remove processing state on error
            }

        } else if (target.matches('.undo-button')) {
            console.log(`Undo Action on Clip ID: ${clipId}`);
            clipItem.classList.add('processing');
            feedbackDiv.textContent = 'Undoing...';
            feedbackDiv.className = 'action-feedback';
            undoButton.style.display = 'none'; // Hide undo while processing undo

             try {
                const response = await fetch(`/api/clips/${clipId}/undo`, {
                    method: 'POST',
                    headers: { 'Accept': 'application/json' }
                });

                const result = await response.json();

                if (!response.ok) {
                    throw new Error(result.detail || `HTTP error! status: ${response.status}`);
                }

                feedbackDiv.textContent = `Success: Reverted to ${result.new_state}. Refresh to see it back in queue.`;
                feedbackDiv.className = 'action-feedback success';
                clipItem.classList.remove('processing', 'done'); // Reset visual state
                // Maybe reload the page or just update UI to indicate it's back
                // For simplicity, just leave the message. User can refresh.


            } catch (error) {
                console.error('Undo failed:', error);
                feedbackDiv.textContent = `Undo Error: ${error.message}`;
                feedbackDiv.className = 'action-feedback error';
                clipItem.classList.remove('processing');
                 // Show undo button again if undo failed? Or just leave error.
            }
        }
    });
});