import paho.mqtt.client as mqtt
import json
import sys
import re
import queue
import time
import threading
from datetime import datetime
from urllib.parse import urlparse

class MQTTJsonStream:
    def __init__(self, config):
        # 1. Initialize ALL attributes first to avoid AttributeError
        self.config = config
        self.topic = config.get('topic', 'agent/logs').rstrip('/')
        self.line_buffer = ""          
        self.connected = False         
        self.msg_queue = queue.Queue()
        self.lock = threading.Lock()   
        self.stitch_timer = None
    
        # Tuning for 1000+ character messages
        self.stitch_delay = 0.1        
        self.max_char_limit = 1000

        # Regex patterns
        self.ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
        self.box_re = re.compile(r'[━┃┏┓┗┛]')

        try:
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
        except AttributeError:
            self.client = mqtt.Client()

        raw_url = config.get('url', 'localhost')
        if config.get('username') and config.get('password'):
            self.client.username_pw_set(config['username'], config['password'])
            
        parsed = urlparse(raw_url) if "://" in raw_url else urlparse(f"mqtt://{raw_url}")
        host = parsed.hostname or "localhost"
        port = parsed.port or config.get('port', 1883)

        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        
        try:
            self.client.connect(host, int(port), keepalive=120)
            self.client.loop_start() 
        except Exception as e:
            sys.__stdout__.write(f"[MQTT Error] {e}\n")

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            self.connected = True
            while not self.msg_queue.empty():
                try:
                    self.client.publish(self.topic, self.msg_queue.get(), qos=1)
                except: pass
        else:
            sys.__stdout__.write(f"[MQTT Refused] RC: {rc}\n")

    def _on_disconnect(self, client, userdata, rc, properties=None):
        self.connected = False

    def publish_now(self):
        """Timer/Thread callback: grabs current buffer and sends it."""
        with self.lock:
            text_to_send = self.line_buffer
            self.line_buffer = ""
            if self.stitch_timer:
                self.stitch_timer.cancel()
                self.stitch_timer = None
        
        if text_to_send.strip():
            self.publish_json(text_to_send)

    def publish_json(self, text):
        # 1. Cleaning logic
        text = self.ansi_escape.sub('', text)
        text = self.box_re.sub('', text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = text.replace('â', '').strip()
        
        if not text: return

        payload = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "agent_id": self.config.get("agent_id", "Test_Agent")
        }

        # 2. Handle Object extraction (Filename + JSON)
        try:
            # re.DOTALL allows the '.' to match newlines inside your 1000+ char data
            json_match = re.search(r'\{.*\}', text, re.DOTALL)
            if json_match:
                payload["data"] = json.loads(json_match.group())
                payload["label"] = text[:json_match.start()].strip()
            else:
                payload["message"] = text
        except:
            payload["message"] = text
        
        mqtt_string = json.dumps(payload)
        
        if self.connected:
            try:   
                self.client.publish(self.topic, mqtt_string, qos=1)
            except Exception as e:
                sys.__stdout__.write(f"[Publish Fail] {e}\n")
        else:
            self.msg_queue.put(mqtt_string)

    def write(self, m):
        sys.__stdout__.write(m) 
    
        with self.lock:
            self.line_buffer += m
            
            # If buffer is large, send immediately in a background thread
            if len(self.line_buffer) >= self.max_char_limit:
                threading.Thread(target=self.publish_now).start()
                return

            # Reset timer for "stitch" (grouping filename + data)
            if self.stitch_timer:
                self.stitch_timer.cancel()
            
            self.stitch_timer = threading.Timer(self.stitch_delay, self.publish_now)
            self.stitch_timer.start()

    def flush(self):
        sys.__stdout__.flush()

    def stop(self):
        self.publish_now() # Final flush of remaining buffer
        time.sleep(1) 
        self.client.loop_stop()
        self.client.disconnect()

def setup_mqtt(config_data):
    streamer = MQTTJsonStream(config_data['mqtt'])
    sys.stdout = streamer
    return streamer