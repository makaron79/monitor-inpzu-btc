import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date
import time
import os

# ---- PARAMETRY ----
ROZNICA_THRESHOLD = 3000.0
CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)

# kanał ntfy.sh – możesz zmienić nazwę
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "inpzu-alert-wojtas")


def http_get_with_retry(url, max_retries=3, timeout=20, sleep_sec=2):
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            last_exc = e
            print(f"[HTTP] {url} próba {attempt} nieudana: {e}")
            if attempt < max_retries:
                time.sleep(sleep_sec)
            else:
                print(f"[HTTP] Błąd po {max_retries} próbach.")
                raise last_exc


# ---- BTC SPOT (aktualny kurs) ----
def fetch_btc_spot():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
    r = http_get_with_retry(url)
    j = r.json()
    price = float(j["bitcoin"]["usd"])
    return price


# ---- NAV inPZU Bitcoin (ostatni dostępny) ----
def fetch_inpzu_nav():
    url = "https://www.biznesradar.pl/notowania-historyczne/PZUSGW.TFI"
    end = date.today()
    start = end - timedelta(days=10)  # ostatnie 10 dni dla pewności

    r = http_get_with_retry(url)
    soup = BeautifulSoup(r.text, "lxml")

    rows = []
    tables = soup.find_all("table")
    for t in tables:
        for tr in t.find_all("tr"):
            cols = [
                td.get_text(strip=True).replace("\xa0", " ")
                for td in tr.find_all(["td", "th"])
            ]
            if len(cols) < 2:
                continue
            date_str = None
            val_str = None
            for c in cols:
                if "." in c and any(ch.isdigit() for ch in c) and len(c) >= 8:
                    date_str = c
                cleaned = (
                    c.replace(" ", "")
                    .replace("PLN", "")
                    .replace("zł", "")
                    .replace("%", "")
                    .replace(",", ".")
                )
                if cleaned.replace(".", "").isdigit():
                    val_str = cleaned
            if date_str and val_str:
                try:
                    d = pd.to_datetime(date_str, dayfirst=True).date()
                    v = float(val_str)
                    if start <= d <= end:
                        rows.append((d, v))
                except Exception:
                    continue

    if not rows:
        print("Brak danych NAV inPZU z BiznesRadar.")
        return None, None

    df = (
        pd.DataFrame(rows, columns=["date", "nav_pln"])
        .drop_duplicates(subset=["date"])
        .sort_values("date")
    )

    # bierzemy OSTATNI dostępny NAV
    latest_nav = df.iloc[-1]
    return latest_nav["nav_pln"], latest_nav["date"]


# ---- SPRAWDZANIE RÓŻNICY I POWIADOMIENIE ----
def check_and_notify():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Sprawdzam różnicę...")

    nav_pln, nav_date = fetch_inpzu_nav()
    if nav_pln is None:
        print("Nie udało się pobrać NAV inPZU. Kończę.")
        return

    btc_now = fetch_btc_spot()
    roznica = nav_pln - btc_now

    print(f"NAV inPZU ({nav_date}): {nav_pln:.4f}")
    print(f"BTC teraz: {btc_now:.4f}")
    print(f"RÓŻNICA: {roznica:.4f}")

    # ZAPIS DO CSV (każde uruchomienie)
    out_path = "intraday_diff_inpzu_vs_btc.csv"
    row = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "nav_date": nav_date,
        "nav_pln": nav_pln,
        "btc_now": btc_now,
        "roznica": roznica,
    }

    if os.path.exists(out_path):
        df_old = pd.read_csv(out_path)
        df_new = pd.concat([df_old, pd.DataFrame([row])], ignore_index=True)
    else:
        df_new = pd.DataFrame([row])
    df_new.to_csv(out_path, index=False)
    print(f"Zapisano do {out_path}")

    # ALERT jeśli różnica >= 3000
    if abs(roznica) >= ROZNICA_THRESHOLD:
        send_ntfy_alert(nav_date, nav_pln, btc_now, roznica)
    else:
        print(f"Różnica {roznica:.2f} < {ROZNICA_THRESHOLD} – brak alertu.")


# ---- POWIADOMIENIE ntfy.sh ----
def send_ntfy_alert(nav_date, nav_pln, btc_now, roznica):
    topic = NTFY_TOPIC.strip()
    if not topic:
        print("Brak nazwy kanału ntfy (NTFY_TOPIC).")
        return

    message = (
        f"🚨 ALERT inPZU vs BTC 🚨\n\n"
        f"Data NAV: {nav_date}\n"
        f"inPZU NAV: {nav_pln:.4f} PLN\n"
        f"BTC teraz: {btc_now:.4f} USD\n"
        f"✅ RÓŻNICA: {roznica:.2f}"
    )

    url = f"https://ntfy.sh/{topic}"
    try:
        r = requests.post(url, data=message.encode("utf-8"), timeout=10)
        r.raise_for_status()
        print(f"✅ Powiadomienie ntfy wysłane na kanał: {topic}")
    except Exception as e:
        print(f"❌ Błąd ntfy: {e}")


def main():
    print("=" * 60)
    print("MONITORING inPZU Bitcoin vs BTC")
    print("=" * 60)
    check_and_notify()
    print("=" * 60)


if __name__ == "__main__":
    main()
