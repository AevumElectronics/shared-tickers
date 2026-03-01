import requests
import os
import json
import time
from stock_calculations2 import perform_calculations  # tieni il tuo modulo

# ────────────────────────────────────────────────
# Configurazioni
# ────────────────────────────────────────────────
API_KEY = os.environ.get("TWELVE_DATA_API_KEY")
if not API_KEY:
    print("ERRORE: TWELVE_DATA_API_KEY non trovata nelle variabili d'ambiente")
    exit(1)

PROJECT_ID = os.environ.get("FIREBASE_PROJECT_ID")
if not PROJECT_ID:
    print("ERRORE: FIREBASE_PROJECT_ID non trovato nelle variabili d'ambiente")
    exit(1)

OUTPUT_DIR = 'data'
os.makedirs(OUTPUT_DIR, exist_ok=True)

CALCULATED_FILE = 'calculatedData.json'

# ────────────────────────────────────────────────
# Legge i ticker da Firestore via REST (sola lettura pubblica)
# ────────────────────────────────────────────────
def get_tickers_from_firestore():
    url = f"https://firestore.googleapis.com/v1/projects/{PROJECT_ID}/databases/(default)/documents/tickers_global/global"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        fields = data.get("fields", {})
        set_map = fields.get("set", {}).get("mapValue", {}).get("fields", {})

        tickers = []
        for key, value in set_map.items():
            if value.get("booleanValue") is True:
                tickers.append(key)

        print(f"Trovati {len(tickers)} ticker attivi")
        if tickers:
            print("Primi 5:", ", ".join(tickers[:5]))
        return tickers

    except Exception as e:
        print(f"Errore nella lettura di Firestore: {e}")
        return []

# ────────────────────────────────────────────────
# Recupera dati da Twelve Data
# ────────────────────────────────────────────────
def recupera_dati_titolo(symbol):
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": "1day",
        "apikey": API_KEY,
        "outputsize": 1000
    }

    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()

        if "values" not in data:
            print(f"Nessun dato valido per {symbol}: {data.get('message', 'nessun messaggio')}")
            return None

        return data

    except Exception as e:
        print(f"Errore API Twelve Data per {symbol}: {e}")
        return None

# ────────────────────────────────────────────────
# Salva i dati e aggiorna calculatedData.json
# ────────────────────────────────────────────────
def salva_dati(symbol, raw_data):
    if not raw_data:
        return

    calculated = perform_calculations(raw_data)
    raw_data['calculated'] = calculated

    # File per simbolo
    filename = os.path.join(OUTPUT_DIR, f"{symbol}.json")
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(raw_data, f, indent=4, ensure_ascii=False)
    print(f"Salvato: {filename}")

    # Aggiorna calculatedData.json
    calc_data = {}
    try:
        with open(CALCULATED_FILE, "r", encoding="utf-8") as f:
            calc_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    calc_data[symbol] = calculated

    with open(CALCULATED_FILE, "w", encoding="utf-8") as f:
        json.dump(calc_data, f, indent=4, ensure_ascii=False)
    print(f"Calcoli aggiornati per {symbol} in {CALCULATED_FILE}")

# ────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────
def main():
    tickers = get_tickers_from_firestore()
    if not tickers:
        print("Nessun ticker da elaborare. Fine.")
        return

    request_count = 0

    for i, symbol in enumerate(tickers):
        print(f"[{i+1:3d}/{len(tickers)}] {symbol}")
        dati = recupera_dati_titolo(symbol)

        if dati:
            salva_dati(symbol, dati)

        request_count += 1

        # Rate limit Twelve Data (8 richieste → pausa)
        if request_count >= 8 and i < len(tickers) - 1:
            print("   → Limite rate: attesa 70 secondi...")
            time.sleep(70)
            request_count = 0

    print("Elaborazione completata.")

if __name__ == "__main__":
    main()
