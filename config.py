import os
import logging
from dotenv import load_dotenv

# Load .env relative to this config file's location (assuming project root)
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=dotenv_path)
print(f"Attempted loading .env from: {dotenv_path}") # Debug print

# --- Logging Setup ---
log = logging.getLogger("meatspace_api") # Specific logger name for the API
if not log.hasHandlers():
    handler = logging.StreamHandler()
    # Example format - adjust as needed
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - [%(module)s:%(lineno)d] - %(message)s')
    handler.setFormatter(formatter)
    log.addHandler(handler)
log.setLevel(logging.INFO)


# --- Configuration Values ---
DATABASE_URL = os.getenv("DATABASE_URL")
DEFAULT_MODEL_NAME = os.getenv("DEFAULT_MODEL_NAME", "openai/clip-vit-base-patch32")
DEFAULT_GENERATION_STRATEGY = os.getenv("DEFAULT_GENERATION_STRATEGY", "keyframe_midpoint")
NUM_RESULTS = int(os.getenv("NUM_RESULTS", 10))
CLOUDFRONT_DOMAIN = os.getenv("CLOUDFRONT_DOMAIN")

# --- Input Validation ---
if not DATABASE_URL:
    # Log critical error and raise
    log.critical("FATAL: DATABASE_URL not found in environment variables. Application cannot start.")
    raise ValueError("DATABASE_URL not found in environment variables.")
if not CLOUDFRONT_DOMAIN:
    # Log critical error and raise
    log.critical("FATAL: CLOUDFRONT_DOMAIN not found in environment variables. Application cannot start.")
    raise ValueError("CLOUDFRONT_DOMAIN not found in environment variables.")

# --- Log Loaded Configuration ---
log.info("--- API Configuration Loaded ---")
log.info(f"Using Database URL (partially hidden): {DATABASE_URL[:15]}...")
log.info(f"Using CloudFront Domain: {CLOUDFRONT_DOMAIN}")
log.info(f"Default Model: {DEFAULT_MODEL_NAME}")
log.info(f"Default Strategy: {DEFAULT_GENERATION_STRATEGY}")
log.info(f"Number of Results: {NUM_RESULTS}")
log.info("-----------------------------")