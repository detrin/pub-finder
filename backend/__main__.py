import uvicorn

from .config import HOST, PORT

uvicorn.run("backend.app:app", host=HOST, port=PORT)
