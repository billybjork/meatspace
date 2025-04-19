import time

def make_throttled_logger(logger, min_interval_sec=2.0):
    """
    Returns a yt-dlp progress_hook function that logs at most once every min_interval_sec,
    or when percent changes.
    """
    last_logged = {"time": 0, "percent": None}

    def hook(d):
        if d.get("status") != "downloading":
            return

        now = time.time()
        percent = d.get('_percent_str', '')
        if (now - last_logged["time"] >= min_interval_sec) or (percent != last_logged["percent"]):
            logger.info(
                f"yt-dlp: downloading {percent} {d.get('_speed_str', '')} ETA {d.get('_eta_str', '')}"
            )
            last_logged["time"] = now
            last_logged["percent"] = percent

    return hook