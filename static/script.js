document.addEventListener('DOMContentLoaded', () => {
    console.log("DOM loaded. Setting up hover listeners."); // Initial check
    const mediaContainers = document.querySelectorAll('.media-container');
    let playTimeout = null;

    mediaContainers.forEach((container, index) => {
        const video = container.querySelector('.video-playback');
        const videoSrc = container.dataset.videoSrc;

        // Basic check for elements
        if (!video) {
            console.warn(`Container ${index}: Video element not found.`);
            return; // Skip if no video element
        }
        if (!videoSrc) {
            console.warn(`Container ${index}: data-video-src attribute missing or empty.`);
            return; // Skip if no src specified
        }

        // Pre-setting src is good, especially with preload="metadata"
        // Check if it's already set to avoid reloading metadata unnecessarily
        if (video.getAttribute('src') !== videoSrc) {
             video.src = videoSrc;
             console.log(`Container ${index}: Set video src to ${videoSrc}`);
        } else {
             console.log(`Container ${index}: Video src ${videoSrc} already set.`);
        }


        container.addEventListener('mouseenter', () => {
            console.log(`Mouse Enter on Container ${index}, video: ${videoSrc}`);
            // Clear any previous timeout to prevent queueing plays
            clearTimeout(playTimeout);

            playTimeout = setTimeout(() => {
                console.log(`Container ${index}: Timeout finished. Attempting to play ${videoSrc}`);
                video.currentTime = 0; // Start from beginning
                const playPromise = video.play();

                if (playPromise !== undefined) {
                    playPromise.then(() => {
                        console.log(`Container ${index}: Playback started successfully for ${videoSrc}`);
                    }).catch(error => {
                        // CCatch and log potential play errors
                        console.error(`Container ${index}: Playback failed for ${videoSrc}:`, error);
                    });
                } else {
                    console.warn(`Container ${index}: video.play() did not return a promise for ${videoSrc}.`);
                }
            }, 150); // Keep the delay for now
        });

        container.addEventListener('mouseleave', () => {
            console.log(`Mouse Leave from Container ${index}, video: ${videoSrc}`);
            clearTimeout(playTimeout); // Clear the play timeout if mouse leaves quickly
            video.pause();
            console.log(`Container ${index}: Paused ${videoSrc}`);
        });
    });
});