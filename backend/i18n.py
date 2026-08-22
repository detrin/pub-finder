from contextvars import ContextVar

from fastapi.templating import Jinja2Templates

SUPPORTED_LOCALES = {"en", "cs"}
DEFAULT_LOCALE = "en"
_current_locale: ContextVar[str] = ContextVar("current_locale", default=DEFAULT_LOCALE)

TRANSLATIONS = {
    "en": {
        "nav.home": "Home",
        "nav.how_it_works": "How it works",
        "nav.feedback": "Feedback",
        "nav.language": "Language",
        "footer.created_by": "Created by",
        "home.made_for_prague": "Made for Prague",
        "home.title": "Find a place that works for everyone.",
        "home.description": "Add starting stops and see their approximate shared reach across Prague, then create a plan when the date, return journey, and live ranking matter.",
        "home.lets_meet": "Let's meet",
        "home.somewhere": "Somewhere",
        "home.quick_estimate": "Quick estimate",
        "home.approximate_one_way": "Approximate · one way",
        "home.starting_stops": "Starting stops",
        "home.add_prague_stop": "Add a Prague stop",
        "home.selected_stops": "Selected starting stops",
        "home.preview_empty": "Add a starting stop to see its reach.",
        "home.preview_one": "Approximate reach from {stop}",
        "home.preview_group": "Shared reach for {count} starting points",
        "home.preview_one_prompt": "Add another stop to see where everyone can reach.",
        "home.preview_group_prompt": "Colour shows the longest estimated journey among the selected starts.",
        "home.preview_disclosure": (
            "Stop-to-stop transit times plus estimated walking from the nearest stop. "
            "No selected date, service changes, or trip home."
        ),
        "home.preview_handoff": "Plan with live DPP times ↓",
        "home.legend_label": "Approximate one-way journey time bands",
        "home.legend_20": "up to 20 min",
        "home.legend_about_35": "21–35 min",
        "home.legend_36_50": "36–50 min",
        "home.legend_51_65": "51–65 min",
        "home.legend_over_65": "over 65 min",
        "home.legend_no_estimate": "no estimate",
        "home.preview_carry": "{count} selected starts will be added to this plan.",
        "home.preview_coverage": "No estimate is available from {stop}. Remove it to continue.",
        "home.preview_duplicate": "That stop is already selected.",
        "home.preview_failure": "The quick estimate is unavailable. You can still create a plan.",
        "home.preview_invalid": "Choose a stop from the Prague stop list.",
        "home.preview_limit": (
            "The quick estimate supports up to six starting stops. For larger groups, start a plan."
        ),
        "home.preview_remove": "Remove {stop}",
        "home.preview_updated_group": "Estimate updated for {count} starting stops.",
        "home.preview_updated_one": "Estimate updated for one starting stop.",
        "home.preview_updating": "Updating estimate…",
        "home.preview_map_label": "Interactive approximate reach map",
        "home.start_plan": "Start a plan",
        "home.plan_support": "Choose a date and time, rank meeting points, and include the trip home.",
        "home.plan_name": "Plan name",
        "home.your_name": "Your name",
        "home.start_planning": "Start planning",
        "home.join_with_code": "Join with a code",
        "home.plan_code": "Plan code",
        "home.join_plan": "Join plan",
        "home.preview_stops_invalid_title": "Choose up to six Prague stops from the list.",
        "home.preview_stops_invalid_body": "No plan was created. Remove the invalid starting stop and try again.",
        "home.try_again": "Try again",
        "how.homepage_quick_estimate_title": "Homepage quick estimate",
        "how.homepage_quick_estimate_body": (
            "The homepage quick estimate uses precomputed typical one-way transit times and "
            "adds estimated walking from the nearest stop. It does not use a selected date, "
            "account for service changes, include a return trip, or call live DPP or Google "
            "services. Create a plan for date-specific journey queries and ranked meeting points."
        ),
        "feedback.page_title": "Feedback",
        "feedback.eyebrow": "Field report",
        "feedback.title": "Tell us what happened",
        "feedback.intro": (
            "Report a problem, a result that looked wrong, or an idea that would make planning easier."
        ),
        "feedback.useful_details": "Useful details",
        "feedback.detail_expected": "What you expected and what actually happened",
        "feedback.detail_context": "Your browser and device",
        "feedback.detail_session": "The Session URL, if the feedback concerns a specific plan",
        "feedback.email": "Email",
        "feedback.optional": "Optional",
        "feedback.email_hint": "Add an email only if you would like a reply.",
        "feedback.session_url": "Session URL",
        "feedback.session_hint": "Paste the full plan link so we can inspect the same session.",
        "feedback.rating": "Rating",
        "feedback.rating_option": "{value} out of 5 stars",
        "feedback.rating_hint": "Choose 0–5 stars, or leave this blank.",
        "feedback.message": "What happened or what should work better?",
        "feedback.message_hint": "Include the steps you took and enough detail to reproduce the issue.",
        "feedback.privacy": (
            "Your feedback is sent to Formspree, a third-party form service. "
            "Do not include sensitive personal information."
        ),
        "feedback.send": "Send feedback",
        "session.plan": "Plan",
        "session.invite": "Invite",
        "session.people": "People",
        "session.participants": "Participants ({count})",
        "session.shared_plan": "Shared plan",
        "session.when_and_what": "When and what",
        "session.departure_date": "Departure date",
        "session.departure_time": "Departure time",
        "session.return_date": "Return date",
        "session.return_time": "Return time",
        "session.occasion": "Occasion",
        "session.drinks": "Drinks",
        "session.coffee": "Coffee",
        "session.food": "Food",
        "session.anything": "Anything",
        "session.method": "Method",
        "session.direction": "Direction",
        "session.find": "Find somewhere",
        "session.add_person": "Add person",
        "session.start": "Start",
        "session.end": "End",
        "session.return_same": "Return to the same stop",
        "session.choose_stop": "Choose a stop",
        "session.select_stop": "Select a stop",
        "session.filter_stops": "Filter stops",
        "session.close": "Close",
        "session.remove": "Remove",
        "session.cancel": "Cancel",
        "session.remove_participant": "Remove participant?",
        "session.remove_detail": "Remove {name} and their selected stops.",
        "session.needs_participant": "Add one more participant.",
        "session.needs_name": "Name each participant.",
        "session.needs_stops": "{name} needs {detail}.",
        "session.needs_start_and_end": "{name} needs start and end stops.",
        "session.needs_end": "{name} needs an end stop.",
        "session.participant": "A participant",
        "session.ready": "Everyone is ready.",
        "session.saved": "saved",
        "session.saving": "saving",
        "session.name_placeholder": "Name",
        "session.pubs": "Pubs",
        "session.bars": "Bars",
        "session.cafes": "Cafes",
        "session.restaurants": "Restaurants",
        "session.minimize_longest": "Minimize longest journey",
        "session.minimize_total": "Minimize total journey",
        "session.round_trip": "Round trip",
        "session.there_only": "There only",
        "session.back_only": "Back only",
        "session.searching": "Searching",
        "progress.preparing": "Preparing search.",
        "progress.back": "Back to the plan",
        "progress.candidates": "Select candidates from the transit matrix.",
        "progress.scraping": "Query DPP journey times. {current} of {total} candidate stops checked.",
        "progress.pubs": "Query nearby places{types} across {total} top stops. {current} of {total} stops checked.",
        "progress.stages": "Search stages",
        "progress.label": "Search progress",
    },
    "cs": {
        "nav.home": "Domů",
        "nav.how_it_works": "Jak to funguje",
        "nav.feedback": "Zpětná vazba",
        "nav.language": "Jazyk",
        "footer.created_by": "Vytvořil",
        "home.made_for_prague": "Pro Prahu",
        "home.title": "Najděte místo, které vyhovuje všem.",
        "home.description": "Přidejte výchozí zastávky a prohlédněte si jejich přibližný společný dosah po Praze. Potom vytvořte plán, když záleží na datu, cestě zpět a aktuálním pořadí míst.",
        "home.lets_meet": "Sejděme se",
        "home.somewhere": "Někde tady",
        "home.quick_estimate": "Rychlý odhad",
        "home.approximate_one_way": "Přibližně · jedním směrem",
        "home.starting_stops": "Výchozí zastávky",
        "home.add_prague_stop": "Přidejte pražskou zastávku",
        "home.selected_stops": "Vybrané výchozí zastávky",
        "home.preview_empty": "Přidejte výchozí zastávku a zobrazí se její dosah.",
        "home.preview_one": "Přibližný dosah ze zastávky {stop}",
        "home.preview_group": "Společný dosah pro {count} výchozích míst",
        "home.preview_one_prompt": "Přidejte další zastávku a podívejte se, kam se dostanou všichni.",
        "home.preview_group_prompt": "Barva ukazuje nejdelší odhadovanou cestu z vybraných výchozích míst.",
        "home.preview_disclosure": (
            "Časy veřejné dopravy mezi zastávkami plus odhad chůze od nejbližší zastávky, "
            "bez zvoleného data, změn v provozu a cesty zpět."
        ),
        "home.preview_handoff": "Plánovat podle aktuálních časů DPP ↓",
        "home.legend_label": "Pásma přibližné doby cesty jedním směrem",
        "home.legend_20": "do 20 min",
        "home.legend_about_35": "21–35 min",
        "home.legend_36_50": "36–50 min",
        "home.legend_51_65": "51–65 min",
        "home.legend_over_65": "nad 65 min",
        "home.legend_no_estimate": "bez odhadu",
        "home.preview_carry": "{count} vybraných výchozích míst bude přidáno do tohoto plánu.",
        "home.preview_coverage": "Pro zastávku {stop} není odhad dostupný. Odeberte ji a pokračujte.",
        "home.preview_duplicate": "Tato zastávka už je vybraná.",
        "home.preview_failure": "Rychlý odhad není dostupný. Plán můžete přesto vytvořit.",
        "home.preview_invalid": "Vyberte zastávku ze seznamu pražských zastávek.",
        "home.preview_limit": (
            "Rychlý odhad podporuje nejvýše šest výchozích zastávek. "
            "Větší skupiny mohou pokračovat vytvořením plánu."
        ),
        "home.preview_remove": "Odebrat zastávku {stop}",
        "home.preview_updated_group": "Odhad byl aktualizován pro {count} výchozích míst.",
        "home.preview_updated_one": "Odhad byl aktualizován pro jednu výchozí zastávku.",
        "home.preview_updating": "Aktualizuji odhad…",
        "home.preview_map_label": "Interaktivní mapa přibližného dosahu",
        "home.start_plan": "Začít plán",
        "home.plan_support": "Zvolte datum a čas, seřaďte místa setkání a zahrňte cestu zpět.",
        "home.plan_name": "Název plánu",
        "home.your_name": "Vaše jméno",
        "home.start_planning": "Začít plánovat",
        "home.join_with_code": "Připojit se kódem",
        "home.plan_code": "Kód plánu",
        "home.join_plan": "Připojit se k plánu",
        "home.preview_stops_invalid_title": "Vyberte ze seznamu nejvýše šest pražských zastávek.",
        "home.preview_stops_invalid_body": "Plán nebyl vytvořen. Odeberte neplatnou výchozí zastávku a zkuste to znovu.",
        "home.try_again": "Zkusit znovu",
        "how.homepage_quick_estimate_title": "Rychlý odhad na úvodní stránce",
        "how.homepage_quick_estimate_body": (
            "Rychlý odhad na úvodní stránce používá předem vypočítané obvyklé doby cest "
            "veřejnou dopravou jedním směrem a přidává odhad chůze od nejbližší zastávky. "
            "Nevyužívá zvolené datum, nezohledňuje změny v provozu, nezahrnuje cestu zpět ani "
            "nevolá aktuální služby DPP či Googlu. Pro dotazy na cesty k určitému datu a "
            "seřazená místa setkání vytvořte plán."
        ),
        "feedback.page_title": "Zpětná vazba",
        "feedback.eyebrow": "Hlášení z terénu",
        "feedback.title": "Napište, co se stalo",
        "feedback.intro": (
            "Nahlaste problém, podezřelý výsledek nebo nápad, který by usnadnil plánování."
        ),
        "feedback.useful_details": "Co nám pomůže",
        "feedback.detail_expected": "Co jste očekávali a co se skutečně stalo",
        "feedback.detail_context": "Váš prohlížeč a zařízení",
        "feedback.detail_session": "URL relace, pokud se zpětná vazba týká konkrétního plánu",
        "feedback.email": "E-mail",
        "feedback.optional": "Nepovinné",
        "feedback.email_hint": "E-mail přidejte pouze v případě, že chcete odpověď.",
        "feedback.session_url": "URL relace",
        "feedback.session_hint": "Vložte celý odkaz na plán, abychom mohli zkontrolovat stejnou relaci.",
        "feedback.rating": "Hodnocení",
        "feedback.rating_option": "{value} z 5 hvězdiček",
        "feedback.rating_hint": "Vyberte 0–5 hvězdiček, nebo hodnocení nechte prázdné.",
        "feedback.message": "Co se stalo nebo co by mělo fungovat lépe?",
        "feedback.message_hint": "Popište provedené kroky a přidejte dostatek údajů pro zopakování problému.",
        "feedback.privacy": (
            "Zpětná vazba se odesílá službě Formspree, která formulář zpracovává. "
            "Neuvádějte citlivé osobní údaje."
        ),
        "feedback.send": "Odeslat zpětnou vazbu",
        "session.plan": "Plán",
        "session.invite": "Pozvat",
        "session.people": "Lidé",
        "session.participants": "Účastníci ({count})",
        "session.shared_plan": "Společný plán",
        "session.when_and_what": "Kdy a co",
        "session.departure_date": "Datum odjezdu",
        "session.departure_time": "Čas odjezdu",
        "session.return_date": "Datum návratu",
        "session.return_time": "Čas návratu",
        "session.occasion": "Příležitost",
        "session.drinks": "Nápoje",
        "session.coffee": "Káva",
        "session.food": "Jídlo",
        "session.anything": "Cokoli",
        "session.method": "Metoda",
        "session.direction": "Směr",
        "session.find": "Najít místo",
        "session.add_person": "Přidat člověka",
        "session.start": "Začátek",
        "session.end": "Konec",
        "session.return_same": "Návrat na stejnou zastávku",
        "session.choose_stop": "Vyberte zastávku",
        "session.select_stop": "Zvolte zastávku",
        "session.filter_stops": "Filtrovat zastávky",
        "session.close": "Zavřít",
        "session.remove": "Odebrat",
        "session.cancel": "Zrušit",
        "session.remove_participant": "Odebrat účastníka?",
        "session.remove_detail": "Odebrat {name} a jeho vybrané zastávky.",
        "session.needs_participant": "Přidejte ještě jednoho účastníka.",
        "session.needs_name": "Pojmenujte každého účastníka.",
        "session.needs_stops": "{name} potřebuje {detail}.",
        "session.needs_start_and_end": "{name} potřebuje začáteční i cílovou zastávku.",
        "session.needs_end": "{name} potřebuje cílovou zastávku.",
        "session.participant": "Účastník",
        "session.ready": "Všichni jsou připraveni.",
        "session.saved": "uloženo",
        "session.saving": "ukládám",
        "session.name_placeholder": "Jméno",
        "session.pubs": "Hospody",
        "session.bars": "Bary",
        "session.cafes": "Kavárny",
        "session.restaurants": "Restaurace",
        "session.minimize_longest": "Minimalizovat nejdelší cestu",
        "session.minimize_total": "Minimalizovat celkovou cestu",
        "session.round_trip": "Celá cesta",
        "session.there_only": "Pouze tam",
        "session.back_only": "Pouze zpět",
        "session.searching": "Hledání",
        "progress.preparing": "Připravuji hledání.",
        "progress.back": "Zpět k plánu",
        "progress.candidates": "Vybírám kandidáty z dopravní matice.",
        "progress.scraping": "Zjišťuji časy spojů DPP. Zkontrolováno {current} z {total} zastávek.",
        "progress.pubs": "Hledám místa v okolí{types} u {total} nejlepších zastávek. Zkontrolováno {current} z {total} zastávek.",
        "progress.stages": "Fáze hledání",
        "progress.label": "Průběh hledání",
    },
}


def set_current_locale(locale: str):
    return _current_locale.set(locale if locale in SUPPORTED_LOCALES else DEFAULT_LOCALE)


def reset_current_locale(token: object) -> None:
    _current_locale.reset(token)


def current_locale() -> str:
    return _current_locale.get()


def translate(key: str, locale: str | None = None, **values: object) -> str:
    locale = locale or current_locale()
    text = TRANSLATIONS.get(locale, TRANSLATIONS[DEFAULT_LOCALE]).get(
        key, TRANSLATIONS[DEFAULT_LOCALE].get(key, key)
    )
    return text.format(**values)


def make_templates(directory: str = "templates") -> Jinja2Templates:
    templates = Jinja2Templates(directory=directory)
    templates.env.globals["t"] = translate
    templates.env.globals["locale"] = current_locale
    return templates
