import uvicorn

from .app import app
from .config import HOST, PORT

uvicorn.run("backend.app:app", host=HOST, port=PORT, reload=True)
