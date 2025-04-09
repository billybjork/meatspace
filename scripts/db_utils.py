import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env file in the parent directory
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path=dotenv_path)

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found in environment variables. Check your .env file.")

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except psycopg2.OperationalError as e:
        print(f"ERROR: Could not connect to database using DATABASE_URL.")
        print(f"Check connection details and ensure the database server is running.")
        print(f"Error details: {e}")
        raise # Re-raise the exception to halt execution if connection fails

def get_media_base_dir():
    """Gets the media base directory from environment variables."""
    media_dir = os.getenv("MEDIA_BASE_DIR")
    if not media_dir:
        raise ValueError("MEDIA_BASE_DIR not found in environment variables. Check your .env file.")
    if not os.path.isdir(media_dir):
         raise ValueError(f"MEDIA_BASE_DIR '{media_dir}' does not exist or is not a directory.")
    return os.path.abspath(media_dir) # Return absolute path for safety