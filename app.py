from flask import Flask, jsonify, request
import yfinance as yf
from datetime import datetime, timedelta
from scipy.interpolate import interp1d
import numpy as np
import requests
import json
import os

app = Flask(__name__)

# === CONFIG ===
FINNHUB_API_KEY = 'd0ahjohr01qm3l9lfmlgd0ahjohr01qm3l9lfmm0'
CACHE_FILE = 'earnings_cache.json'

# === UTILITIES ===

def filter_dates(dates):
    today = datetime.today().date()
    cutoff_date = today + timedelta(days=45)
    sorted_dates = sorted(datetime.strptime(date, "%Y-%m-%d").date() for date in dates)

    arr = []
    for i, date in enumerate(sorted_dates):
        if date >= cutoff_date:
            arr = [d.strftime("%Y-%m-%d") for d in sorted_dates[:i+1]]
            break

    if len(arr) > 0:
        if arr[0] == today.strftime("%Y-%m-%d"):
            return arr[1:]
        return arr

    raise ValueError("No date 45 days or more in the future found.")

def yang_zhang(price_data, window=30, trading_periods=252):
    log_ho = np.log(price_data['High'] / price_data['Open'])
    log_lo = np.log(price_data['Low'] / price_data['Open'])
    log_co = np.log(price_data['Close'] / price_data['Open'])
    log_oc = np.log(price_data['Open'] / price_data['Close'].shift(1))
    log_oc_sq = log_oc**2
    log_cc = np.log(price_data['Close'] / price_data['Close'].shift(1))
    log_cc_sq = log_cc**2
    rs = log_ho * (log_ho - log_co) + log_lo * (log_lo - log_co)

    close_vol = log_cc_sq.rolling(window).sum() / (window - 1.0)
    open_vol = log_oc_sq.rolling(window).sum() / (window - 1.0)
    window_rs = rs.rolling(window).sum() / (window - 1.0)

    k = 0.34 / (1.34 + ((window + 1) / (window - 1)))
    result = (open_vol + k * close_vol + (1 - k) * window_rs).apply(np.sqrt) * np.sqrt(trading_periods)

    return result.dropna().iloc[-1]

def build_term_structure(days, ivs):
    days = np.array(days)
    ivs = np.array(ivs)

    sort_idx = days.argsort()
    days = days[sort_idx]
    ivs = ivs[sort_idx]

    spline = interp1d(days, ivs, kind='linear', fill_value="extrapolate")

    def term_spline(dte):
        if dte < days[0]:
            return ivs[0]
        elif dte > days[-1]:
            return ivs[-1]
        else:
            return float(spline(dte))

    return term_spline

# === ROUTES ===

@app.route('/screen', methods=['GET'])
def screen():
    ticker = request.args.get('ticker', 'AAPL')
    try:
        stock = yf.Ticker(ticker)
        if len(stock.options) == 0:
            return jsonify({'error': 'No options data found.'})

        exp_dates = filter_dates(stock.options)
        options_chains = {date: stock.option_chain(date) for date in exp_dates}

        spot = stock.history(period='1d')['Close'][0]
        atm_iv = {}

        for exp_date, chain in options_chains.items():
            calls, puts = chain.calls, chain.puts
            if calls.empty or puts.empty:
                continue
            call_idx = (calls['strike'] - spot).abs().idxmin()
            put_idx = (puts['strike'] - spot).abs().idxmin()
            call_iv = calls.loc[call_idx, 'impliedVolatility']
            put_iv = puts.loc[put_idx, 'impliedVolatility']
            atm_iv[exp_date] = (call_iv + put_iv) / 2.0

        if not atm_iv:
            return jsonify({'error': 'No ATM IVs found.'})

        today = datetime.today().date()
        dtes = [(datetime.strptime(date, "%Y-%m-%d").date() - today).days for date in atm_iv.keys()]
        ivs = list(atm_iv.values())
        term_spline = build_term_structure(dtes, ivs)

        iv30 = term_spline(30)
        iv90 = term_spline(90)
        slope = (iv90 - iv30) / (90 - 30)

        price_history = stock.history(period='3mo')
        rv30 = yang_zhang(price_history)
        iv30_rv30 = iv30 / rv30
        avg_volume = price_history['Volume'].rolling(30).mean().dropna().iloc[-1]

        return jsonify({
            'ticker': ticker,
            'iv30': iv30,
            'iv90': iv90,
            'slope': slope,
            'iv30_rv30': iv30_rv30,
            'average_volume': avg_volume
        })

    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/earnings', methods=['GET'])
def get_recent_earnings():
    try:
        with open(CACHE_FILE, "r") as f:
            data = json.load(f)
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/update_cache', methods=['GET'])
def update_cache():
    try:
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, "r") as f:
                cached = json.load(f)
        else:
            cached = []

        cached_keys = {(e["symbol"], e["date"]) for e in cached}

        today = datetime.utcnow().date()
        yesterday = today - timedelta(days=1)
        tomorrow = today + timedelta(days=1)

        url = "https://finnhub.io/api/v1/calendar/earnings"

        params_yesterday = {'from': yesterday.isoformat(), 'to': yesterday.isoformat(), 'token': FINNHUB_API_KEY}
        params_tomorrow = {'from': tomorrow.isoformat(), 'to': tomorrow.isoformat(), 'token': FINNHUB_API_KEY}

        r1 = requests.get(url, params=params_yesterday)
        r2 = requests.get(url, params=params_tomorrow)

        earnings_yesterday = r1.json().get("earningsCalendar", [])
        earnings_tomorrow = r2.json().get("earningsCalendar", [])

        print("Finnhub Yesterday Earnings:", earnings_yesterday)
        print("Finnhub Tomorrow Earnings:", earnings_tomorrow)

        new_entries = []

        for e in earnings_yesterday:
            if e.get("hour") == "amc":
                key = (e["symbol"], yesterday.isoformat())
                if key not in cached_keys:
                    new_entries.append({
                        "symbol": e["symbol"],
                        "date": yesterday.isoformat(),
                        "hour": "amc"
                    })

        for e in earnings_tomorrow:
            if e.get("hour") == "bmo":
                key = (e["symbol"], tomorrow.isoformat())
                if key not in cached_keys:
                    new_entries.append({
                        "symbol": e["symbol"],
                        "date": tomorrow.isoformat(),
                        "hour": "bmo"
                    })

        cached.extend(new_entries)

        with open(CACHE_FILE, "w") as f:
            json.dump(cached, f, indent=2)

        return jsonify({
            "message": f"Added {len(new_entries)} new earnings entries.",
            "new_entries": new_entries
        })

    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/wake', methods=['GET'])
def wake():
    return jsonify({"message": "I'm awake!"})
    
# === RUN SERVER ===

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
