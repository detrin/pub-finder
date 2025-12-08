import requests
from bs4 import BeautifulSoup


def get_route_info(start_place: str, end_place: str, date: str, time: str, infinite_time: int = 24 * 60) -> int:
    cookies = {
        'consent_analytics_storage': 'denied',
        'consent_is_set': 'true',
        'consent_updated_at': '2025-12-06T17:22:15.744Z',
    }

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9,sk;q=0.8,cs;q=0.7,ko;q=0.6',
        'Connection': 'keep-alive',
        'DNT': '1',
        'Referer': 'https://www.dpp.cz/',
        'Sec-Fetch-Dest': 'iframe',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-site',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
    }

    params = {
        'res': '1',
        'tom': '0',
        'cl': 'CS',
        'f': start_place,
        't': end_place,
        'date': date,
        'time': time,
        'isdep': '0',
        'f_time': time,
    }

    url = 'https://spojeni.dpp.cz/'
    response = requests.get(url, params=params, cookies=cookies, headers=headers, timeout=15)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.content, 'html.parser')
    # save html
    with open("dpp.html", "w") as f:
        f.write(response.content.decode("utf-8"))
    
    # Check if the response indicates no connection was found
    if "Nepodařilo se vyhledat vhodné spojení" in response.text:
        return infinite_time
    
    travel_times = []
    for div in soup.find_all("div"):
        classes = div.get("class") or []
        if "Box-ticket" in classes:
            for span in div.find_all("span"):
                text = span.get_text(strip=True)
                if "Doba jízdy" in text:
                    strong = span.find("strong")
                    if strong:
                        travel_time = strong.get_text(strip=True)
                        if travel_time:
                            travel_times.append(travel_time)
    
    if not travel_times:
        raise ValueError(f"No travel time found for route from {start_place} to {end_place}")
    
    # Parse all travel times and return the minimum
    from backend.utils import parse_time_to_minutes
    parsed_times = [parse_time_to_minutes(time_str) for time_str in travel_times]
    return min(parsed_times)