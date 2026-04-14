import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
import json
import os
import subprocess
import uuid
import sys


# --- Load MQTT Configuration from client.json ---
CONFIG_FILE = "client.json"

try:
    with open(CONFIG_FILE, 'r') as f:
        config = json.load(f)
except FileNotFoundError:
    print(f"❌ Error: {CONFIG_FILE} not found!")
    exit(1)

MQTT_BROKER = config.get("MQTT_BROKER", "localhost")
MQTT_PORT = config.get("MQTT_PORT", 1883)
MQTT_USER = config.get("MQTT_USER", "admin")
MQTT_PW = config.get("MQTT_PW", "password")
TOPIC = config.get("TOPIC", "agno/agent/logs/set")
#SAVE_DIR = config.get("SAVE_DIR", "received")

#
# Track files received in the current cycle
received_files = {"config.json": False, "user_instruction.json": False}

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print(f"Connected to Broker: {MQTT_BROKER}")
        client.subscribe(TOPIC)
    else:
        print(f"Connection failed. Reason Code: {reason_code}")

def on_message(client, userdata, msg):
    global received_files
    
    try:
        data = json.loads(msg.payload.decode())
        
        # Logic to determine which file we are receiving
        # (Assuming the payload contains a 'type' or we alternate)
        # For this version, we save based on which one is missing
        if not received_files["config.json"]:
            filename = "config.json"
        else:
            filename = "user_instruction.json"

        filepath = os.path.join(filename)
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=4)
        
        print(f"Saved: {filename}")
        received_files[filename] = True

        # When both files are present, call the audit script
        if all(received_files.values()):
            print("Both files received. Triggering Agent...")
            try:
                # Runs abcd.py and waits for it to finish
                #subprocess.run(["python", "run_agent.py"], check=True)
                subprocess.run([sys.executable, "run_agent.py"], check=True)
                print("Agent executed successfully.")
            except Exception as e:
                print(f" Error running abcd.py: {e}")
            
            # Reset for the next batch
            received_files = {"config.json": False, "user_instruction.json": False}
            print("Ready for new files...")

    except Exception as e:
        print(f" Processing Error: {e}")

# --- Client Setup ---
unique_id = f"receiver-{uuid.uuid4().hex[:6]}"
client = mqtt.Client(CallbackAPIVersion.VERSION2, client_id=unique_id)
client.username_pw_set(MQTT_USER, MQTT_PW)
client.on_connect = on_connect
client.on_message = on_message

# Keepalive set to 60s for a stable long-term connection
try:
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    print("🛰️ Listener is active. Press Ctrl+C to stop.")
    client.loop_forever() 
except KeyboardInterrupt:
    print("\n Listener stopped by user.")
except Exception as e:
    print(f"Fatal Error: {e}")