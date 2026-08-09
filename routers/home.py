from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from backend.i18n import DEFAULT_LOCALE, SUPPORTED_LOCALES, make_templates

router = APIRouter()
templates = make_templates()


@router.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse(
        request,
        "home.html",
        {"error": request.query_params.get("error")},
    )


@router.get("/language/{locale}")
async def set_language(locale: str, next: str = "/"):
    response = RedirectResponse(url=next if next.startswith("/") else "/", status_code=303)
    response.set_cookie(
        "language",
        locale if locale in SUPPORTED_LOCALES else DEFAULT_LOCALE,
        max_age=60 * 60 * 24 * 365,
        samesite="lax",
    )
    return response


@router.get("/how-it-works", response_class=HTMLResponse)
async def how_it_works(request: Request):
    return templates.TemplateResponse(request, "how_it_works.html")


@router.get("/feedback", response_class=HTMLResponse)
async def feedback(request: Request):
    return templates.TemplateResponse(request, "feedback.html")
