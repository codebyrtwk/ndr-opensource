import json
import boto3
import base64
import numpy as np
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

def pca_analysis(event):
    records = event['Records']
    data = []

    for record in records:
        payload = json.loads(base64.b64decode(record['kinesis']['data']))
        data.append([
            payload['ttl'],
            payload['header_length'],
            payload['src_port'],
            payload['dest_port'],
            payload['size']
        ])

    if len(data) > 0:
        scaler = StandardScaler()
        scaled_data = scaler.fit_transform(data)
        
        pca = PCA(n_components=2)
        principal_components = pca.fit_transform(scaled_data)
        
        pca_results = []
        for pc in principal_components:
            pca_results.append({
                'PC1': pc[0],
                'PC2': pc[1]
            })
        
        # Store the PCA results in S3
        s3 = boto3.client('s3')
        s3.put_object(
            Bucket='your-bucket-name',
            Key='pca_results.json',
            Body=json.dumps(pca_results)
        )
    
    return {
        'statusCode': 200,
        'body': json.dumps('PCA processing complete')
    }
