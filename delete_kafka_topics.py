#!/usr/bin/env python3
"""
Delete Kafka topics from MSK cluster
"""
from kafka.admin import KafkaAdminClient
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import argparse

class MSKTokenProvider:
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token('us-east-1')
        return token

def create_admin_client(bootstrap_servers):
    """Create Kafka admin client with IAM auth"""
    return KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        security_protocol='SASL_SSL',
        sasl_mechanism='OAUTHBEARER',
        sasl_oauth_token_provider=MSKTokenProvider(),
        request_timeout_ms=30000,
        api_version_auto_timeout_ms=10000
    )

def delete_topics(admin_client, topics):
    """Delete specified topics"""
    try:
        # Get existing topics
        existing_topics = admin_client.list_topics()
        topics_to_delete = [topic for topic in topics if topic in existing_topics]
        
        if not topics_to_delete:
            print("No matching topics found to delete")
            return
        
        print(f"Found topics to delete: {topics_to_delete}")
        
        # Confirm deletion
        confirm = input(f"Are you sure you want to delete {len(topics_to_delete)} topic(s)? (yes/no): ")
        if confirm.lower() != 'yes':
            print("Deletion cancelled")
            return
        
        # Delete topics
        delete_result = admin_client.delete_topics(topics_to_delete, timeout_ms=30000)
        
        # Wait for deletion to complete
        for topic, future in delete_result.topic_error_codes.items():
            try:
                future.result()
                print(f"✓ Topic '{topic}' deleted successfully")
            except Exception as e:
                print(f"✗ Failed to delete topic '{topic}': {e}")
                
    except Exception as e:
        print(f"Error during topic deletion: {e}")

def main():
    parser = argparse.ArgumentParser(description='Delete Kafka topics')
    parser.add_argument('--bootstrap-servers', required=True, 
                       help='Kafka bootstrap servers (comma-separated)')
    parser.add_argument('--topics', nargs='+', required=True,
                       help='Topics to delete')
    parser.add_argument('--force', action='store_true',
                       help='Skip confirmation prompt')
    
    args = parser.parse_args()
    
    print(f"Connecting to Kafka: {args.bootstrap_servers}")
    
    try:
        # Create admin client
        admin_client = create_admin_client(args.bootstrap_servers.split(','))
        
        # List current topics
        current_topics = admin_client.list_topics()
        print(f"Current topics: {sorted(current_topics)}")
        
        # Delete topics (with confirmation unless --force)
        if args.force:
            # Skip confirmation for force delete
            existing_topics = admin_client.list_topics()
            topics_to_delete = [topic for topic in args.topics if topic in existing_topics]
            
            if topics_to_delete:
                print(f"Force deleting topics: {topics_to_delete}")
                delete_result = admin_client.delete_topics(topics_to_delete, timeout_ms=30000)
                
                for topic, future in delete_result.topic_error_codes.items():
                    try:
                        future.result()
                        print(f"✓ Topic '{topic}' deleted successfully")
                    except Exception as e:
                        print(f"✗ Failed to delete topic '{topic}': {e}")
            else:
                print("No matching topics found to delete")
        else:
            delete_topics(admin_client, args.topics)
        
        # List remaining topics
        remaining_topics = admin_client.list_topics()
        print(f"\nRemaining topics: {sorted(remaining_topics)}")
        
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
