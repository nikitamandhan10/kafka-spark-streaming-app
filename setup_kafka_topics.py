#!/usr/bin/env python3
"""
Force delete and recreate Kafka topics for MSK cluster
"""
from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType, NewTopic
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import argparse
import time
import boto3

class MSKTokenProvider:
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token('us-east-1')
        return token

def get_bootstrap_servers(cluster_arn):
    """Get bootstrap servers from MSK cluster ARN"""
    try:
        client = boto3.client('kafka', region_name='us-east-1')
        response = client.get_bootstrap_brokers(ClusterArn=cluster_arn)
        
        # Try public IAM first, then private IAM
        if 'BootstrapBrokerStringPublicSaslIam' in response:
            servers = response['BootstrapBrokerStringPublicSaslIam']
            print(f"Using public IAM bootstrap servers: {servers}")
            return servers
        elif 'BootstrapBrokerStringSaslIam' in response:
            servers = response['BootstrapBrokerStringSaslIam']
            print(f"Using private IAM bootstrap servers: {servers}")
            return servers
        else:
            raise ValueError("No IAM-enabled bootstrap servers found")
            
    except Exception as e:
        print(f"Error getting bootstrap servers: {e}")
        raise

def create_admin_client(bootstrap_servers):
    """Create Kafka admin client with IAM auth"""
    return KafkaAdminClient(
        bootstrap_servers=bootstrap_servers.split(','),
        security_protocol='SASL_SSL',
        sasl_mechanism='OAUTHBEARER',
        sasl_oauth_token_provider=MSKTokenProvider(),
        request_timeout_ms=30000,
        api_version_auto_timeout_ms=10000
    )

def delete_topics(admin_client, topics):
    """Delete topics if they exist"""
    try:
        # Get existing topics
        existing_topics = admin_client.list_topics()
        topics_to_delete = [topic for topic in topics if topic in existing_topics]
        
        if topics_to_delete:
            print(f"Deleting topics: {topics_to_delete}")
            delete_result = admin_client.delete_topics(topics_to_delete, timeout_ms=30000)
            
            # Wait for deletion to complete
            for topic, future in delete_result.topic_error_codes.items():
                try:
                    future.result()
                    print(f"✓ Topic '{topic}' deleted successfully")
                except Exception as e:
                    print(f"✗ Failed to delete topic '{topic}': {e}")
        else:
            print("No topics to delete")
            
    except Exception as e:
        print(f"Error during topic deletion: {e}")

def create_topics(admin_client, topics_config):
    """Create topics with specified configuration"""
    try:
        new_topics = []
        for topic_name, config in topics_config.items():
            new_topic = NewTopic(
                name=topic_name,
                num_partitions=config.get('partitions', 3),
                replication_factor=config.get('replication_factor', 2),
                topic_configs=config.get('configs', {})
            )
            new_topics.append(new_topic)
        
        print(f"Creating topics: {list(topics_config.keys())}")
        create_result = admin_client.create_topics(new_topics, timeout_ms=30000)
        
        # Wait for creation to complete
        for topic, future in create_result.topic_errors.items():
            try:
                future.result()
                print(f"✓ Topic '{topic}' created successfully")
            except Exception as e:
                print(f"✗ Failed to create topic '{topic}': {e}")
                
    except Exception as e:
        print(f"Error during topic creation: {e}")
        # Topics might still be created despite the error
        pass

def main():
    parser = argparse.ArgumentParser(description='Force delete and recreate Kafka topics')
    parser.add_argument('--cluster-arn', required=True, 
                       help='MSK Cluster ARN')
    parser.add_argument('--topics', nargs='+', 
                       default=['ecommerce-orders', 'ecommerce-alerts'],
                       help='Topics to recreate')
    
    args = parser.parse_args()
    
    # Topic configurations
    topics_config = {
        'ecommerce-orders': {
            'partitions': 3,
            'replication_factor': 2,
            'configs': {
                'retention.ms': '604800000',  # 7 days
                'cleanup.policy': 'delete'
            }
        },
        'ecommerce-alerts': {
            'partitions': 3,
            'replication_factor': 2,
            'configs': {
                'retention.ms': '259200000',  # 3 days
                'cleanup.policy': 'delete'
            }
        }
    }
    
    # Filter topics based on command line args
    filtered_config = {topic: topics_config[topic] for topic in args.topics 
                      if topic in topics_config}
    
    print(f"Getting bootstrap servers for cluster: {args.cluster_arn}")
    
    try:
        # Get bootstrap servers from cluster ARN
        bootstrap_servers = get_bootstrap_servers(args.cluster_arn)
        
        # Create admin client
        admin_client = create_admin_client(bootstrap_servers)
        
        # Delete existing topics
        delete_topics(admin_client, args.topics)
        
        # Wait a bit for deletion to propagate
        print("Waiting 5 seconds for deletion to complete...")
        time.sleep(5)
        
        # Create new topics
        create_topics(admin_client, filtered_config)
        
        print("\n✓ Topic setup completed successfully!")
        
        # List final topics
        final_topics = admin_client.list_topics()
        print(f"Available topics: {sorted(final_topics)}")
        
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    finally:
        try:
            admin_client.close()
        except:
            pass
    
    return 0

if __name__ == "__main__":
    exit(main())
