import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.metrics import accuracy_score
from mqtt_sender import send_csv_to_mqtt

class NOCAuditor:
    def __init__(self):
        self.model = xgb.XGBClassifier(
            n_estimators=100,
            max_depth=4,
            scale_pos_weight=5, 
            learning_rate=0.1,
            random_state=42
        )
        self.features = ["cpu_usage", "latency_ms", "packet_loss", "alarm_nearby"]

    def get_recommendation(self, row):
        if row['prediction'] == 0:
            return "Stable", "No action"
        
        recs, actions = [], []
        if row['cpu_usage'] > 80:
            recs.append("CPU Thermal/Load Strain")
            actions.append("Check process scheduler")
        if row['packet_loss'] > 2:
            recs.append("Interface CRC Errors")
            actions.append("Reseat SFP/Fiber")
        
        return (" | ".join(recs) if recs else "Transient Anomaly"), \
               ("; ".join(actions) if actions else "Monitor telemetry")

def run_advanced_audit(perf_csv, alarm_csv):
    # Load Data
    perf = pd.read_csv(perf_csv, parse_dates=['timestamp'])
    alarms = pd.read_csv(alarm_csv, parse_dates=['timestamp'])

    # 1. "Nearby Alarm" feature calculation
    def count_nearby(row):
        window_start = row['timestamp'] - pd.Timedelta(minutes=5)
        window_end = row['timestamp'] + pd.Timedelta(minutes=25)
        nearby = alarms[(alarms['device_id'] == row['device_id']) & 
                        (alarms['timestamp'] >= window_start) & 
                        (alarms['timestamp'] <= window_end)]
        return len(nearby)

    perf['alarm_nearby'] = perf.apply(count_nearby, axis=1)

    # 2. Define Metric + Nearby Alarm for training
    y_true = ((perf['cpu_usage'] > 80) & (perf['alarm_nearby'] > 0)).astype(int)

    # 3. ML Processing
    auditor = NOCAuditor()
    auditor.model.fit(perf[auditor.features], y_true)
    
    perf['risk_score'] = auditor.model.predict_proba(perf[auditor.features])[:, 1]
    perf['prediction'] = auditor.model.predict(perf[auditor.features])

    # 4. Generate NOC Descriptions and Actions
    perf[['rca', 'action']] = perf.apply(lambda x: pd.Series(auditor.get_recommendation(x)), axis=1)

    # 5. Export File 1: Full Report
    report_filename = 'final_noc_report.csv'
    perf.to_csv(report_filename, index=False)


    # 6. Metrics
    acc = accuracy_score(y_true, perf['prediction'])
    ml_pos = perf[perf['prediction'] == 1]
    cv_score = (ml_pos['alarm_nearby'] > 0).mean() if len(ml_pos) > 0 else 0.0

     # 7. Export File 2: Recommendations Only
    # Filtering for rows where an issue was actually predicted
    rec_filename = 'final_recommendation.csv'
    recommendation_df = perf[perf['prediction'] == 1][['timestamp', 'device_id', 'risk_score', 'rca', 'action']]
    recommendation_df.to_csv(rec_filename, index=False)



    print("\n" + "="*50 + "\n NOC AUDIT COMPLETED\n" + "="*50)
    print(f"XGBoost Accuracy:      {acc:.2%}")
    print(f"Risks Logged:          {len(recommendation_df)} items")
    print(f"Cross-Verification:    {cv_score:.2%} (Alarms confirmed)")
    print(f"Files Generated:       {report_filename}, {rec_filename}")
    print("="*50)

   

   
    

    # 8. Send files over MQTT
    status, message = send_csv_to_mqtt(rec_filename)
    if status:
        print(f"Successfully sent {rec_filename}: {message}")
    else:
        print(f"Failed to send {rec_filename}: {message}")

if __name__ == "__main__":
    run_advanced_audit('performance_data.csv', 'alarm_data.csv')