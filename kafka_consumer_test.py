#!/usr/bin/env python3
from kafka import KafkaConsumer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import json
import uuid

class MSKTokenProvider:
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token('us-east-1')
        return token

def test_consumer():
    bootstrap_server = "b-1-public.ecommerceanalyticsmsk.0q0f1v.c13.kafka.us-east-1.amazonaws.com:9198"
    
    print(f"Creating consumer for: {bootstrap_server}")
    
    try:
        consumer = KafkaConsumer(
            'ecommerce-orders',
            bootstrap_servers=[bootstrap_server],
            security_protocol='SASL_SSL',
            sasl_mechanism='OAUTHBEARER',
            sasl_oauth_token_provider=MSKTokenProvider(),
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=10000,  # 10 second timeout
            auto_offset_reset='latest',  # Read from beginning
            enable_auto_commit=True,
            group_id='test-consumer-group'
        )
        
        print("Consumer created successfully!")
        
        # Check topic partitions
        partitions = consumer.partitions_for_topic('ecommerce-orders')
        print(f"Topic partitions: {partitions}")
        
        print("Waiting for messages (10 second timeout)...")
        
        message_count = 0
        for message in consumer:
            print(f"Partition {message.partition}, Offset {message.offset}: {message.value}")
            message_count += 1
            if message_count >= 10:  # Stop after 10 messages
                break
                
        if message_count == 0:
            print("No messages received")
            # Check available topics
            try:
                topics = list(consumer.topics())
                print(f"Available topics: {topics}")
            except Exception as e:
                print(f"Could not list topics: {e}")
        else:
            print(f"Successfully read {message_count} messages")
        
        consumer.close()
        
    except Exception as e:
        print(f"Consumer failed: {e}")
        print(f"Error type: {type(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_consumer()
