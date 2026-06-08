#!/usr/bin/env python3
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import time
import os
from io import StringIO

ROZNICA_THRESHOLD = 3000.0
CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)

NTFY_TOPIC = "inpzu-alert-wojtas"
NTFY_URL = f"https://ntfy.sh/{NTFY_TOPIC}"


def http_get_with_retry(url, max_retries=3, timeout=20, sleep_sec=2, headers=None):
    last_exc = None
    headers = headers or {"User-Agent": "Mozilla/5.0"}
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, timeout=timeout, headers=headers)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            last_exc = e
            print(f"[HTTP] {url} próba {attempt} nieudana: {e}")
            if attempt < max_retries:
                time.sleep(sleep_sec)
            else:
                raise last_exc


def fetch_btc_spot():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
    r = http_get_with_retry(url)
    j = r.json()
    return float(j["bitcoin"]["usd"])


def fetch_inpzu_nav():
    url = "https://stooq.pl/q/d/l/?s=1150.n&i=d"
    r = http_get_with_retry(url)
    r.encoding = "utf-8"
    text = r.text.lstrip("\ufeff").strip()

    if "<html" in text.lower() or "<!doctype html" in text.lower():
        print("[Stooq] Odpowiedź wygląda na HTML, próbuję parsować tabelę HTML.")
        try:
            tables = pd.read_html(StringIO(text))
            if not tables:
                print("[Stooq] Brak tabel HTML.")
                return None, None
            df = tables[0]
        except Exception as e:
            print(f"[Stooq] Nie udało się sparsować HTML: {e}")
            return None, None
    else:
        try:
            df = pd.read_csv(StringIO(text), sep=None, engine="python")
        except Exception as e:
            print(f"[Stooq] CSV parsing failed: {e}")
            return None, None

    print("Kolumny ze Stooq:", list(df.columns))
    cols_lower = {str(c).strip().lower(): c for c in df.columns}

    date_col = cols_lower.get("date") or cols_lower.get("data")
    close_col = cols_lower.get("close") or cols_lower.get("zamkniecie") or cols_lower.get("kurs")

    if date_col is None or close_col is None:
        if len(df.columns) >= 5:
            date_col = df.columns[0]
            close_col = df.columns[4]
            print(f"[Stooq] Używam heurystyki: date={date_col}, close={close_col}")
        else:
            print("Nie rozpoznano kolumn daty / kursu w danych Stooq.")
            return None, None

    if df.empty:
        print("Brak danych NAV ze Stooq.")
        return None, None

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.date
    df[close_col] = pd.to_numeric(df[close_col], errors="coerce")
    df = df.dropna(subset=[date_col, close_col]).sort_values(date_col)

    if df.empty:
        print("Brak poprawnych rekordów po czyszczeniu danych Stooq.")
        return None, None

    latest = df.iloc[-1]
    return float(latest[close_col]), latest[date_col]


def fetch_bloomberg_index_ft(timeout=5):
    url = "https://markets.ft.com/data/indices/tearsheet/summary?s=BITCOIN:IOM"
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
    except Exception as e:
        print(f"[FT] Błąd HTTP lub timeout ({timeout}s): {e}")
        return None, None, None

    soup = BeautifulSoup(r.text, "html.parser")
    price_value = None
    change_abs = None
    change_pct = None

    try:
        label_span = soup.find("span", string=lambda s: s and "Price (USD)" in s)
        if label_span:
            li = label_span.find_parent("li")
            if li:
                vspan = li.find("span", class_="mod-ui-data-list__value")
                if vspan:
                    txt = vspan.get_text(strip=True)
                    price_value = float(txt.replace(",", "").replace(" ", ""))
    except Exception as e:
        print(f"[FT] Problem z parsowaniem Price (USD): {e}")

    try:
        label_span = soup.find("span", class_="mod-ui-data-list__label", string=lambda s: s and "Today's Change" in s)
        if label_span:
            li = label_span.find_parent("li")
            if li:
                vspan = li.find("span", class_="mod-ui-data-list__value")
                if vspan:
                    txt = vspan.get_text(strip=True)
                    parts = [p.strip() for p in txt.split("/")]
                    if len(parts) == 2:
                        abs_str = parts[0].replace(",", "").replace(" ", "")
                        pct_str = parts[1].replace("%", "").replace(",", "").replace(" ", "")
                        change_abs = float(abs_str)
                        change_pct = float(pct_str)
    except Exception as e:
        print(f"[FT] Problem z parsowaniem Today's Change: {e}")

    if price_value is None and change_abs is None:
        print("[FT] Nie udało się wiarygodnie odczytać danych BITCOIN:IOM.")
    else:
        print(f"[FT] BITCOIN:IOM Price={price_value}, Change={change_abs} / {change_pct}%")

    return price_value, change_abs, change_pct


def send_ntfy_alert(nav_date, nav_pln, btc_now, roznica,
                    ft_price, ft_change_abs, ft_change_pct):
    topic = NTFY_TOPIC.strip()
    if not topic:
        print("Brak nazwy kanału ntfy (NTFY_TOPIC).")
        return

    ft_lines = ""
    if ft_price is not None:
        ft_lines += f"\nFT BITCOIN:IOM Price (USD): {ft_price:.2f}"
    if ft_change_abs is not None:
        ft_lines += (
            f"\nFT BITCOIN:IOM Today's Change: "
            f"{ft_change_abs:.2f} USD / {ft_change_pct:.2f}%"
        )

    message = (
        "🚨 ALERT inPZU vs BTC 🚨\n\n"
        f"NAV inPZU ({nav_date}): {nav_pln:.4f} PLN\n"
        f"BTC teraz: {btc_now:.4f} USD\n"
        f"RÓŻNICA: {roznica:.4f}"
        f"{ft_lines}"
    )

    url = f"https://ntfy.sh/{topic}"
    try:
        r = requests.post(url, data=message.encode("utf-8"), timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        print(f"✅ Powiadomienie ntfy wysłane na kanał: {topic}")
    except Exception as e:
        print(f"❌ Błąd ntfy: {e}")


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

    ft_price, ft_change_abs, ft_change_pct = fetch_bloomberg_index_ft()
    if ft_price is not None:
        print(f"FT BITCOIN:IOM Price (USD): {ft_price:.2f}")
    if ft_change_abs is not None:
        print(f"FT BITCOIN:IOM Today's Change: {ft_change_abs:.2f} USD / {ft_change_pct:.2f}%")

    out_path = "intraday_diff_inpzu_vs_btc.csv"
    row = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "nav_date": nav_date,
        "nav_pln": nav_pln,
        "btc_now": btc_now,
        "roznica": roznica,
        "ft_price": ft_price,
        "ft_change_abs": ft_change_abs,
        "ft_change_pct": ft_change_pct,
    }

    if os.path.exists(out_path):
        df_old = pd.read_csv(out_path)
        df_new = pd.concat([df_old, pd.DataFrame([row])], ignore_index=True)
    else:
        df_new = pd.DataFrame([row])

    df_new.to_csv(out_path, index=False)
    print(f"Zapisano do {out_path}")

    if abs(roznica) >= ROZNICA_THRESHOLD:
        send_ntfy_alert(nav_date, nav_pln, btc_now, roznica,
                        ft_price, ft_change_abs, ft_change_pct)
    else:
        print(f"Różnica {roznica:.2f} < {ROZNICA_THRESHOLD} – brak alertu.")


def main():
    print("=" * 60)
    print("MONITORING inPZU Bitcoin vs BTC + Bloomberg BITCOIN:IOM z FT")
    print("=" * 60)
    check_and_notify()
    print("=" * 60)


if __name__ == "__main__":
    main()
