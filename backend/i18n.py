from fastapi.templating import Jinja2Templates

SUPPORTED_LOCALES = {"en", "cs"}
DEFAULT_LOCALE = "en"

TRANSLATIONS = {
    "en": {
        "nav.home": "Home",
        "nav.how_it_works": "How it works",
        "nav.feedback": "Feedback",
        "nav.language": "Language",
        "footer.created_by": "Created by",
        "home.made_for_prague": "Made for Prague",
        "home.title": "Find a place that works for everyone.",
        "home.description": "Add people, choose a time, and rank meeting points using Prague public transport times.",
        "home.lets_meet": "Let’s meet",
        "home.somewhere": "Somewhere",
        "home.start_plan": "Start a plan",
        "home.plan_name": "Plan name",
        "home.your_name": "Your name",
        "home.start_planning": "Start planning",
        "home.join_with_code": "Join with a code",
        "home.plan_code": "Plan code",
        "home.join_plan": "Join plan",
    },
    "cs": {
        "nav.home": "Domů",
        "nav.how_it_works": "Jak to funguje",
        "nav.feedback": "Zpětná vazba",
        "nav.language": "Jazyk",
        "footer.created_by": "Vytvořil",
        "home.made_for_prague": "Pro Prahu",
        "home.title": "Najděte místo, které vyhovuje všem.",
        "home.description": "Přidejte lidi, zvolte čas a seřaďte místa setkání podle času v pražské hromadné dopravě.",
        "home.lets_meet": "Sejděme se",
        "home.somewhere": "Někde",
        "home.start_plan": "Začít plán",
        "home.plan_name": "Název plánu",
        "home.your_name": "Vaše jméno",
        "home.start_planning": "Začít plánovat",
        "home.join_with_code": "Připojit se kódem",
        "home.plan_code": "Kód plánu",
        "home.join_plan": "Připojit se k plánu",
    },
}


def translate(key: str, locale: str = DEFAULT_LOCALE, **values: object) -> str:
    text = TRANSLATIONS.get(locale, TRANSLATIONS[DEFAULT_LOCALE]).get(
        key, TRANSLATIONS[DEFAULT_LOCALE].get(key, key)
    )
    return text.format(**values)


def make_templates(directory: str = "templates") -> Jinja2Templates:
    templates = Jinja2Templates(directory=directory)
    templates.env.globals["t"] = translate
    return templates
