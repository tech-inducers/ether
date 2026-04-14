import paho.mqtt.client as mqtt
import json
import csv


import paho.mqtt.client as mqtt
import json
import csv

def load_config(config_path="config.json"):
    with open(config_path, "r") as f:
        return json.load(f)

def send_csv_to_mqtt(csv_file_path):
    try:
        config = load_config()
        m_conf = config['mqtt']
        
        # Clean the URL (remove mqtt:// if present)
        broker_address = m_conf['url'].replace("mqtt://", "")
        
        # Initialize Client with Version 2 API
        client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        
        # Set Credentials
        if m_conf.get('username'):
            client.username_pw_set(m_conf['username'], m_conf['password'])

        # Connect
        client.connect(broker_address, m_conf['port'])
        
        count = 0
        with open(csv_file_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Publish each row as a JSON string
                client.publish(m_conf['topic'], json.dumps(row))
                count += 1
        
        client.disconnect()
        return True, f"Successfully sent {count} rows to {m_conf['topic']}"

    except Exception as e:
        return False, str(e)
    

if __name__ == "__main__":
    send_csv_to_mqtt('final_noc_report.csv')    