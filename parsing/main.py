import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import csv

# --- КАСТОМНАЯ КОНФИГ: три пункта по ТЗ ---
CONFIG = {
    "headers": {"User-Agent": "Mozilla/5.0 (compatible; SimpleScraper/1.0)"},
    "delay_seconds": 0.8,   # задержка между запросами
    "max_retries": 3,       # кол-во повторов при статусе != 200/ошибке
    # не по ТЗ, но полезно:
    "timeout_seconds": 15,
}

def get_with_retries(url: str, cfg: dict) -> str | None:
    """
    Запрос с повторами:
    - возвращает html-строку при 200
    - повторяет попытки при статусе != 200 и сетевых ошибках
    """
    for attempt in range(1, cfg["max_retries"] + 1):
        try:
            r = requests.get(url, headers=cfg["headers"], timeout=cfg["timeout_seconds"])
            if r.status_code == 200:
                return r.text
            else:
                print(f"[{attempt}/{cfg['max_retries']}] status={r.status_code} -> retry")
        except requests.RequestException as e:
            print(f"[{attempt}/{cfg['max_retries']}] error: {e} -> retry")
        time.sleep(cfg["delay_seconds"] * attempt)
    return None

def parse_page(html: str, base_url: str):
    """Возвращает (records, next_url). Минимум 3 поля: текст, автор, теги."""
    soup = BeautifulSoup(html, "html.parser")
    records = []
    for box in soup.select("div.quote"):
        text = (box.select_one("span.text").get_text(strip=True) if box.select_one("span.text") else "").strip("“”")
        author = box.select_one("small.author").get_text(strip=True) if box.select_one("small.author") else ""
        tags = ", ".join(t.get_text(strip=True) for t in box.select("div.tags a.tag"))
        records.append({"quote_text": text, "author": author, "tags": tags})
    # пагинация: если нет Next(переход на другую страницу) — это последняя страница
    nxt = soup.select_one("a[rel=next]") or soup.select_one("li.next > a")
    next_url = urljoin(base_url, nxt["href"]) if nxt and nxt.has_attr("href") else None
    return records, next_url


base = "https://quotes.toscrape.com/"
url = base
all_rows = []

while url:
    html = get_with_retries(url, CONFIG)
    if not html:
        print(f"Не смог получить страницу: {url}")
        break

    rows, url = parse_page(html, base)
    all_rows.extend(rows)

    # задержка между страницами
    time.sleep(CONFIG["delay_seconds"])

print(f"Собрано записей: {len(all_rows)}")

#  сохранить в CSV

with open("quotes.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["quote_text", "author", "tags"])
    writer.writeheader()
    writer.writerows(all_rows)
print("CSV сохранён: quotes.csv")
