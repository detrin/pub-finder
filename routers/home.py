from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse(request, "home.html")


@router.get("/how-it-works", response_class=HTMLResponse)
async def how_it_works(request: Request):
    return templates.TemplateResponse(request, "how_it_works.html")


@router.get("/feedback", response_class=HTMLResponse)
async def feedback(request: Request):
    return templates.TemplateResponse(request, "feedback.html")
