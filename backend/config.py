import os

from dotenv import load_dotenv

load_dotenv()

GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY", "")
DATABASE_PATH = os.getenv("DATABASE_PATH", "pub_finder.db")
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "3000"))
GA4_MEASUREMENT_ID = os.getenv("GA4_MEASUREMENT_ID", "")
GA4_API_SECRET = os.getenv("GA4_API_SECRET", "")
PLACES_DAILY_REQUEST_LIMIT = int(os.getenv("PLACES_DAILY_REQUEST_LIMIT", "100"))
