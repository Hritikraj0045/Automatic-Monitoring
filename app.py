# app.py
from flask import Flask, render_template
from flask_socketio import SocketIO
from threading import Thread
import webbrowser
import logging
import time

import scraping_1sec
import scraping_1min

# -------------------------------------------------------
# FLASK + SOCKETIO SETUP
# -------------------------------------------------------
app = Flask(__name__, static_folder="static", template_folder="templates")
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

logging.basicConfig(level=logging.INFO)


# -------------------------------------------------------
# ROUTES
# -------------------------------------------------------
@app.route("/")
@app.route("/tab1sec")
def tab1sec():
    return render_template("Monitor_page.html", active_tab="tab1sec")

@app.route("/tab1min")
def tab1min():
    return render_template("Monitor_page.html", active_tab="tab1min")

@app.route("/tab5min")
def tab5min():
    return render_template("Monitor_page.html", active_tab="tab5min")


# -------------------------------------------------------
# THREAD STARTERS
# -------------------------------------------------------
def start_1sec():
    scraping_1sec.start_threads(socketio)

def start_1min():
    scraping_1min.start_threads(socketio)


# -------------------------------------------------------
# MAIN ENTRY
# -------------------------------------------------------
if __name__ == "__main__":

    # Start background workers
    Thread(target=start_1sec, daemon=True).start()
    Thread(target=start_1min, daemon=True).start()

    # Allow threads to start before UI opens
    time.sleep(0.5)

    webbrowser.open("http://127.0.0.1:5000/")

    # IMPORTANT — debug=False for stability
    socketio.run(app, host="0.0.0.0", port=5000, debug=False)
