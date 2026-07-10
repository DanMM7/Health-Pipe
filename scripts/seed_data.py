import pandas as pd
import random
from datetime import datetime, timedelta
import boto3

s3 = boto3.client('s3', endpoint_url='http://localhost:4566')

counties = ['Kings', 'Queens', 'Bronx', 'Richmond', 'New York']
severities = ['Minor', 'Moderate', 'Severe', 'Fatal']

for county in counties:
    data = {
        'incident_id': [f"{county[:3]}_{i}" for i in range(100)],
        'county': [county] * 100,
        'date': [(datetime.now() - timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d') 
                 for _ in range(100)],
        'severity': [random.choice(severities) for _ in range(100)],
        'count': [random.randint(1, 50) for _ in range(100)]
    }
    df = pd.DataFrame(data)
    csv_buffer = df.to_csv(index=False)
    s3.put_object(Bucket='county-raw-data', Key=f"{county.lower()}_{datetime.now().strftime('%Y%m%d')}.csv", Body=csv_buffer)

print("Sample data seeded to S3")