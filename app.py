from flask import Flask, jsonify, request

app = Flask(__name__)

@app.route('/screen', methods=['GET'])
def screen():
    # Simulated screener output
    result = {
        "Ticker": "AAPL",
        "Recommendation": "✅ All Conditions Pass",
        "Predictor Variables": {
            "Slope": "-0.0031 ✅",
            "IV/RV": "1.42 ✅",
            "Volume": "3.2M ✅"
        },
        "Expected Move": "5.4%",
        "Strike Selection": "195.0",
        "Trade Setup": {
            "Short Leg": "195C [Jun 28]",
            "Long Leg": "195C [Sep 20]",
            "Entry Debit": "$2.40"
        },
        "Macro Status": "✅ VIX flat, XLK bullish"
    }
    return jsonify(result)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
