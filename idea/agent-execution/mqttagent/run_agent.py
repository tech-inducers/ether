import json
import os
import sqlite3
import requests
import re
from pathlib import Path
from agno.agent import Agent
from agno.models.ollama import Ollama
from agno.tools.csv_toolkit import CsvTools
from agno.tools.python import PythonTools
from agno.tools.shell import ShellTools
from agno.tools.file import FileTools
from agno.tools.duckduckgo import DuckDuckGoTools
from mqtt_logger import setup_mqtt 


def get_ctx(m):
    try:
        r = requests.post("http://localhost:11434/api/show", json={"name": m, "verbose": True}, timeout=2)
        match = re.search(r'num_ctx\s+(\d+)', r.json().get("parameters", ""))
        return int(match.group(1)) if match else 4096
    except: return 4096

def run():
    
    p = Path.cwd()
    c_path = p.parent / "config.json" if (p.parent / "config.json").exists() else p / "config.json"
    
    with open(c_path, 'r') as f:
        config = json.load(f)
    
    # Initialize MQTT Logger
    logger_instance = setup_mqtt(config)
    #setup_mqtt(config)
    try:
        model_id = config.get('ollama_model', 'phi4')
        
        limit = 4096 or get_ctx(model_id)
       
        
        print(f"--- SESSION START | Model: {model_id} | Limit: {limit} ---")

        #agent = Agent(model=Ollama(id=model_id), tools=[DuckDuckGoTools()], markdown=True)

        

        imdb_csv = Path(__file__).parent.joinpath("alarm_data.csv")
        imdb_csv.parent.mkdir(parents=True, exist_ok=True)

        imdb1_csv = Path(__file__).parent.joinpath("performance_data.csv")
        imdb1_csv.parent.mkdir(parents=True, exist_ok=True)

       
        
        base_dir = Path(__file__).parent
        print("base_dir" , base_dir)
    
        agent = Agent(model=Ollama(id=model_id,options={"temperature": 0}), 
       
        tools=[DuckDuckGoTools(),PythonTools(base_dir=base_dir), ShellTools(base_dir=base_dir),FileTools(base_dir=base_dir),CsvTools(enable_query_csv_file=False, csvs=[imdb_csv,imdb1_csv])],
       
        #markdown=True,
        stream=True,
        
       # debug_mode=True
        
       
        )

        #agent.parse_response

        d_path = p.parent / config['dataset_path'] if (p.parent / config['dataset_path']).exists() else p / config['dataset_path']
        with open(d_path, 'r') as f: data = json.load(f)
        
        items = data.get("items", [])
        if len(json.dumps(data)) > (limit * 3.5):
            print("Status: Payload > Context. Buffering to SQLite.")
            db = "temp_context.db"
            conn = sqlite3.connect(db)
            conn.execute("CREATE TABLE IF NOT EXISTS b (c TEXT)")
            for i in items: conn.execute("INSERT INTO b VALUES (?)", (json.dumps(i),))
            conn.commit()
            for row in conn.execute("SELECT c FROM b"):
               response =  agent.print_response(f"Process: {row[0]}")
               
            conn.close()
            os.remove(db)
        else:
           response =  agent.print_response(json.dumps(data))
          
        print("\\n--- SESSION COMPLETE ---")    
    except Exception as e:
        print(f"An error occurred: {e}")
        
    finally:
        # This is the critical part to prevent the deallocator error
        print("\n---  Shutting down logger ---")
        if logger_instance:
            logger_instance.stop()    

if __name__ == "__main__":
    run()