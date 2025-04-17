document.addEventListener('DOMContentLoaded', () => {
    console.log("Meatspace script loaded. Setting up listeners.");

    // --- Dropdown Selector Logic ---
    const optionsSelect = document.getElementById('model-strategy-select');
    if (optionsSelect) {
        console.log("Found options selector dropdown.");
        optionsSelect.addEventListener('change', function() {
            const selectedValue = this.value;
            if (!selectedValue || !selectedValue.includes('|')) {
                console.warn("Invalid option selected in dropdown:", selectedValue);
                return;
            }

            const [model, strategy] = selectedValue.split('|');

            // Get the current base path (e.g., /query/some_clip_id)
            // Avoid including existing query params in the base path
            const currentPath = window.location.pathname;

            // Construct the new URL with updated query parameters
            const newUrl = `${currentPath}?model=${encodeURIComponent(model)}&strategy=${encodeURIComponent(strategy)}`;

            console.log("Dropdown changed. Redirecting to:", newUrl);
            window.location.href = newUrl; // Redirect the browser
        });
    } else {
        console.log("Options selector dropdown not found on this page.");
    }

    // --- Hover Playback Logic ---
    console.log("Setting up hover listeners for media containers.");
    const mediaContainers = document.querySelectorAll('.media-container');
    let playTimeout = null; // Define timeout variable outside the loop

    mediaContainers.forEach((container, index) => {
        const video = container.querySelector('.video-playback');
        const image = container.querySelector('.keyframe-thumb'); // Need the image element too
        const videoSrc = container.dataset.videoSrc;

        // Basic check for elements
        if (!video) {
            console.warn(`Container ${index}: Video element not found.`);
            return; // Skip if no video element
        }
        if (!image) {
            console.warn(`Container ${index}: Image element not found.`);
            // Decide if you want to proceed without image hiding/showing
            // return; // Or continue cautiously
        }
        if (!videoSrc) {
            console.warn(`Container ${index}: data-video-src attribute missing or empty.`);
            return; // Skip if no src specified
        }

        // Pre-setting src - Check if it's already set to avoid unnecessary resets
        // Using getAttribute is slightly more robust than .src property for initial check
        if (video.getAttribute('src') !== videoSrc) {
             video.src = videoSrc;
             // console.log(`Container ${index}: Set video src to ${videoSrc}`);
        } else {
             // console.log(`Container ${index}: Video src ${videoSrc} already set.`);
        }

        container.addEventListener('mouseenter', () => {
            // Clear any previous timeout for *this specific container* if needed,
            // but a single global timeout might be simpler if overlaps aren't common.
            // Using a single timeout means only the *last* hovered item will try to play after the delay.
            clearTimeout(playTimeout);

            playTimeout = setTimeout(() => {
                // console.log(`Container ${index}: Timeout finished. Attempting to play ${videoSrc}`);
                video.currentTime = 0; // Start from beginning
                const playPromise = video.play();

                if (playPromise !== undefined) {
                    playPromise.then(() => {
                        // Hide image, show video ONLY on successful play start
                        if (image) image.style.opacity = '0';
                        video.style.opacity = '1';
                    }).catch(error => {
                        // Catch and log potential play errors (e.g., user interaction needed)
                        console.error(`Container ${index}: Playback failed for ${videoSrc}:`, error);
                        // Ensure visual state is reset if play fails
                        if (image) image.style.opacity = '1';
                        video.style.opacity = '0';
                    });
                } else {
                    // Fallback for browsers not returning a promise (very old?)
                    console.warn(`Container ${index}: video.play() did not return a promise for ${videoSrc}. Assuming playback started.`);
                    if (image) image.style.opacity = '0';
                    video.style.opacity = '1';
                }
            }, 150); // Delay before attempting play
        });

        container.addEventListener('mouseleave', () => {
            // console.log(`Mouse Leave from Container ${index}, video: ${videoSrc}`);
            clearTimeout(playTimeout); // Clear the play timeout if mouse leaves quickly
            video.pause();
            // console.log(`Container ${index}: Paused ${videoSrc}`);
            video.currentTime = 0; // Optional: Reset to start on mouse leave

            // Reset visual state: Show image, hide video
            if (image) image.style.opacity = '1';
            video.style.opacity = '0';
        });
    });

    console.log(`Finished setting up hover listeners for ${mediaContainers.length} containers.`);
});