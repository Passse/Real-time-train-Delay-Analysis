import json
import os
import time
from datetime import datetime
from pathlib import Path

import stomp
from dotenv import load_dotenv

load_dotenv()

HOST = "publicdatafeeds.networkrail.co.uk"
PORT = 61618
TOPIC = "/topic/TRAIN_MVT_ALL_TOC"

USERNAME = os.getenv("NR_USERNAME")
PASSWORD = os.getenv("NR_PASSWORD")

OUTPUT_DIR = Path("../../../data/raw_real")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

class MovementListener(stomp.ConnectionListener):
    def on_error(self, frame):
        print("ERROR headers:", dict(frame.headers))
        print("ERROR body:", frame.body)

    def on_connected(self, frame):
        print("Connected:", dict(frame.headers))

    def on_message(self, frame):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        output_file = OUTPUT_DIR / f"movement_{timestamp}.json"

        body = frame.body

        try:
            parsed = json.loads(body)
            with output_file.open("w", encoding="utf-8") as f:
                json.dump(parsed, f, indent=2)
            print(f"Movement data written to {output_file}")
        except json.JSONDecodeError:
            # retain the original message as .txt, if body is not a normal JSON
            raw_file = OUTPUT_DIR / f"movement_{timestamp}_raw.txt"
            raw_file.write_text(body, encoding="utf-8")
            print(f"Saved raw message {raw_file}")

def main():
    if not USERNAME or not PASSWORD:
        raise ValueError("Please provide both username and password in the .env file")

    conn = stomp.Connection12([(HOST, PORT)], heartbeats=(5000,5000))
    conn.set_listener("", MovementListener())

    conn.connect(
        username=USERNAME,
        passcode=PASSWORD,
        wait=True
    )

    conn.subscribe(
        destination=TOPIC,
        id="movement-subscription",
        ack="auto"
    )

    print(f"Subscribed to {TOPIC}")
    print("Collecting messages. Press CTRL+C to stop")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping collector...")
    finally:
        conn.disconnect()

if __name__ == "__main__":
    main()