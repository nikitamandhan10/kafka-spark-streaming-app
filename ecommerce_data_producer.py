#!/usr/bin/env python3
import json
import random
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer
import argparse
import boto3
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

class MSKTokenProvider:
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token('us-east-1')
        return token

def get_msk_bootstrap_servers(cluster_arn):
    """Get MSK bootstrap servers"""
    client = boto3.client('kafka', region_name='us-east-1')
    response = client.get_bootstrap_brokers(ClusterArn=cluster_arn)
    
    # Prioritize public endpoints for local access
    if 'BootstrapBrokerStringPublicSaslIam' in response:
        return response['BootstrapBrokerStringPublicSaslIam']
    elif 'BootstrapBrokerStringSaslIam' in response:
        return response['BootstrapBrokerStringSaslIam']
    else:
        raise KeyError("No suitable bootstrap broker string found")

def create_producer(bootstrap_servers):
    """Create Kafka producer with SASL/IAM auth"""
    first_broker = bootstrap_servers.split(',')[0]
    
    return KafkaProducer(
        bootstrap_servers=[first_broker],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        security_protocol='SASL_SSL',
        sasl_mechanism='OAUTHBEARER',
        sasl_oauth_token_provider=MSKTokenProvider(),
        acks='all',
        retries=3,
        request_timeout_ms=30000,
        api_version_auto_timeout_ms=10000
    )

def generate_order():
    """Generate a sample e-commerce order"""
    products = [
        {"id": "P001", "name": "Laptop", "price": 999.99},
        {"id": "P002", "name": "Phone", "price": 699.99},
        {"id": "P003", "name": "Headphones", "price": 199.99},
        {"id": "P004", "name": "Tablet", "price": 399.99},
        {"id": "P005", "name": "Watch", "price": 299.99}
    ]
    
    product = random.choice(products)
    quantity = random.randint(1, 5)
    
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": f"CUST_{random.randint(1000, 9999)}",
        "product_id": product["id"],
        "product_name": product["name"],
        "quantity": quantity,
        "price": product["price"],
        "total_amount": product["price"] * quantity,
        "timestamp": datetime.now().isoformat(),
        "region": random.choice(["US-East", "US-West", "EU", "Asia"])
    }

def main():
    parser = argparse.ArgumentParser(description='E-commerce data producer')
    parser.add_argument('--cluster-arn', required=True, help='MSK Cluster ARN')
    parser.add_argument('--duration', type=int, default=60, help='Duration in seconds')
    
    args = parser.parse_args()
    
    # Get bootstrap servers
    bootstrap_servers = get_msk_bootstrap_servers(args.cluster_arn)
    print(f"Connecting to MSK servers: {bootstrap_servers}")
    
    # Create producer
    producer = create_producer(bootstrap_servers)
    print("Producer created successfully!")
    
    # Generate and send messages
    message_count = 0
    start_time = time.time()
    
    try:
        while time.time() - start_time < args.duration:
            order = generate_order()
            
            # Send to ecommerce-orders topic
            producer.send('ecommerce-orders', value=order)
            message_count += 1
            
            print(f"Sent order {message_count}: {order['order_id']} - ${order['total_amount']:.2f}")
            
            # Send alert for high-value orders
            if order['total_amount'] > 1000:
                alert = {
                    "alert_type": "HIGH_VALUE_ORDER",
                    "order_id": order['order_id'],
                    "amount": order['total_amount'],
                    "timestamp": datetime.now().isoformat()
                }
                producer.send('ecommerce-alerts', value=alert)
                print(f"  -> High value alert sent!")
            
            time.sleep(0.5)  # Send every 500ms
            
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()
        producer.close()
        print(f"Producer closed. Total messages sent: {message_count}")

if __name__ == "__main__":
    main()
