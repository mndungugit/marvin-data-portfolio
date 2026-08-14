import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

jobs = [
    {'job_id': '1', 'name': 'BackupDB_Production', 'category': 'Maintenance'},
    {'job_id': '2', 'name': 'UpdateStatistics', 'category': 'Maintenance'},
    {'job_id': '3', 'name': 'SyncReports', 'category': 'ETL'},
    {'job_id': '4', 'name': 'ArchiveOldRecords', 'category': 'Maintenance'},
    {'job_id': '5', 'name': 'ValidateDataIntegrity', 'category': 'Health'},
    {'job_id': '6', 'name': 'ExportDailyMetrics', 'category': 'ETL'},
    {'job_id': '7', 'name': 'CacheRefresh', 'category': 'Optimization'},
    {'job_id': '8', 'name': 'IndexMaintenance', 'category': 'Maintenance'},
]

data = []
end_date = datetime.now()
start_date = end_date - timedelta(days=30)

for job in jobs:
    current_date = start_date
    while current_date <= end_date:
        if job['category'] == 'Maintenance':
            run_times = [2, 6, 14, 22]
        elif job['category'] == 'ETL':
            run_times = [1, 7, 13, 19]
        elif job['category'] == 'Health':
            run_times = [3, 15]
        else:
            run_times = [0, 12]

        for hour in run_times:
            run_datetime = current_date.replace(hour=hour, minute=random.randint(0, 59), second=0)
            
            success = random.random() > 0.1
            status = 'Succeeded' if success else 'Failed'
            
            if success:
                duration = random.randint(30, 600)
            else:
                duration = random.randint(5, 120)
            
            retries = random.randint(0, 2) if not success else 0
            
            data.append({
                'run_date': run_datetime.strftime('%Y-%m-%d'),
                'run_time': run_datetime.strftime('%H:%M:%S'),
                'run_datetime': run_datetime,
                'job_id': job['job_id'],
                'job_name': job['name'],
                'category': job['category'],
                'status': status,
                'duration_seconds': duration,
                'retries': retries,
                'message': 'Job completed successfully' if success else 'Error: timeout or data validation failure'
            })
        
        current_date += timedelta(days=1)

df = pd.DataFrame(data)
df = df.sort_values('run_datetime').reset_index(drop=True)

df.to_csv('sample_job_history.csv', index=False)
print(f"✓ Generated {len(df)} job run records")
print(f"✓ Date range: {df['run_date'].min()} to {df['run_date'].max()}")
print(f"✓ Saved to: sample_job_history.csv")
print(f"\nFirst 5 rows:")
print(df.head())