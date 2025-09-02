# Real-Time E-Commerce Transaction Analytics

A comprehensive AWS-native real-time analytics pipeline that processes e-commerce transactions using Amazon MSK (Managed Streaming for Apache Kafka) and Apache Spark Structured Streaming on Amazon EMR. The system ingests simulated e-commerce orders and produces windowed Parquet aggregates stored in S3 with sub-5-second processing latency.

## Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Producer │───▶│   Amazon MSK    │───▶│   EMR Spark     │───▶│   Amazon S3     │
│  (Python/Kafka) │    │   (Kafka)       │    │  Streaming      │    │  (Parquet)      │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │  Alert Topic    │    │  Fraud Detection│
                       │   (Kafka)       │    │   & Monitoring  │
                       └─────────────────┘    └─────────────────┘
```

## Features

- **Real-time Stream Processing**: Sub-5-second latency for transaction processing
- **Fault-tolerant Architecture**: Spark checkpointing for reliable stream processing
- **Windowed Aggregations**: 1-minute sliding windows with 30-second intervals
- **Fraud Detection**: Real-time alerts for high-value orders and suspicious patterns
- **Scalable Storage**: Parquet format in S3 for efficient analytics
- **IAM Authentication**: Secure MSK access using AWS IAM
- **Monitoring**: Console output and metrics for operational visibility

## Project Structure

```
├── ecommerce_data_producer.py    # Kafka producer for simulated e-commerce data
├── ecommerce_streaming.py        # Main Spark Structured Streaming application
├── setup_kafka_topics.py        # Topic creation and configuration
├── delete_kafka_topics.py       # Topic cleanup utility
├── kafka_consumer_test.py       # Consumer testing utility
├── emr-bootstrap-python-setup.sh # EMR cluster bootstrap script
└── README.md                    # This file
```

## Prerequisites

- AWS Account with appropriate permissions
- Amazon MSK cluster with IAM authentication enabled
- Amazon EMR cluster (6.x or later)
- S3 buckets for data storage and checkpointing
- Python 3.7+ with required dependencies

## Dependencies

```bash
pip install kafka-python==2.0.2 boto3 aws-msk-iam-sasl-signer-python pyspark
```

## Setup Instructions

### 1. Create MSK Cluster

```bash
# Create MSK cluster with IAM authentication
aws kafka create-cluster \
    --cluster-name ecommerce-analytics-msk \
    --broker-node-group-info file://broker-config.json \
    --client-authentication file://client-auth.json
```

### 2. Setup Kafka Topics

```bash
python setup_kafka_topics.py --cluster-arn <MSK_CLUSTER_ARN>
```

This creates two topics:
- `ecommerce-orders`: Main transaction stream (3 partitions, 7-day retention)
- `ecommerce-alerts`: Fraud alerts stream (3 partitions, 3-day retention)

### 3. Launch EMR Cluster

```bash
aws emr create-cluster \
    --name "ECommerce-Analytics-EMR" \
    --release-label emr-6.15.0 \
    --applications Name=Spark \
    --instance-type m5.xlarge \
    --instance-count 3 \
    --bootstrap-actions Path=s3://your-bucket/emr-bootstrap-python-setup.sh
```

### 4. Upload Application Code

```bash
aws s3 cp ecommerce_streaming.py s3://your-bucket/scripts/
```

## Usage

### Start Data Producer

Generate simulated e-commerce transactions:

```bash
python ecommerce_data_producer.py \
    --cluster-arn <MSK_CLUSTER_ARN> \
    --duration 300
```

**Sample Output:**
```
Sent order 1: a1b2c3d4-e5f6-7890-abcd-ef1234567890 - $1299.95
  -> High value alert sent!
Sent order 2: b2c3d4e5-f6g7-8901-bcde-f23456789012 - $399.99
```

### Run Streaming Analytics

Submit the Spark job to EMR:

```bash
spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
    ecommerce_streaming.py \
    --kafka-servers <BOOTSTRAP_SERVERS> \
    --s3-bucket <OUTPUT_BUCKET> \
    --checkpoint-bucket <CHECKPOINT_BUCKET>
```

### Test Consumer

Verify message consumption:

```bash
python kafka_consumer_test.py
```

## Data Schema

### Order Schema
```json
{
  "order_id": "uuid",
  "customer_id": "CUST_1234",
  "product_id": "P001",
  "product_name": "Laptop",
  "quantity": 2,
  "price": 999.99,
  "total_amount": 1999.98,
  "timestamp": "2024-01-01T12:00:00",
  "region": "US-East"
}
```

### Aggregated Metrics
```json
{
  "window_start": "2024-01-01T12:00:00",
  "window_end": "2024-01-01T12:01:00",
  "category": "Electronics",
  "location": "US-East",
  "order_count": 15,
  "total_revenue": 12450.75,
  "avg_order_value": 830.05,
  "unique_customers": 12,
  "max_order_value": 1999.98,
  "min_order_value": 199.99
}
```

## Monitoring & Operations

### Key Metrics
- **Processing Latency**: < 5 seconds end-to-end
- **Throughput**: 1000+ orders/minute
- **Window Size**: 1-minute sliding windows
- **Checkpoint Interval**: 10 seconds

### Fraud Detection Rules
- High-value orders (> $1000)
- Suspicious locations
- Simulated fraud patterns

### Console Output Example
```
+-------------------+-------------------+------------+----------+-----------+-------------+
|       window_start|         window_end|    category|  location|order_count|total_revenue|
+-------------------+-------------------+------------+----------+-----------+-------------+
|2024-01-01 12:00:00|2024-01-01 12:01:00|Electronics|    US-East|         15|     12450.75|
|2024-01-01 12:00:30|2024-01-01 12:01:30|   Clothing|    US-West|          8|      3200.40|
+-------------------+-------------------+------------+----------+-----------+-------------+
```

## Configuration

### Spark Configuration
- **Adaptive Query Execution**: Enabled for optimal performance
- **Kryo Serialization**: Faster serialization for streaming
- **State Store**: HDFS-backed for fault tolerance
- **Watermarking**: 30-second late data tolerance

### Kafka Configuration
- **Security**: SASL_SSL with IAM authentication
- **Batch Size**: 1000 records per trigger
- **Timeout**: 60-second request timeout
- **Retry Policy**: 3 retries with exponential backoff

## Troubleshooting

### Common Issues

1. **Authentication Errors**
   ```bash
   # Verify IAM permissions for MSK access
   aws sts get-caller-identity
   ```

2. **Topic Not Found**
   ```bash
   # Recreate topics
   python setup_kafka_topics.py --cluster-arn <ARN>
   ```

3. **Checkpoint Recovery**
   ```bash
   # Clear checkpoints if needed
   aws s3 rm s3://checkpoint-bucket/checkpoint-path/ --recursive
   ```

### Logs and Debugging
- EMR logs: `/var/log/spark/`
- Application logs: Spark UI and CloudWatch
- Kafka consumer lag: MSK monitoring

## Performance Tuning

### Spark Optimizations
- Increase `maxOffsetsPerTrigger` for higher throughput
- Adjust `processingTime` trigger intervals
- Tune partition count based on data volume

### Kafka Optimizations
- Configure appropriate retention policies
- Monitor consumer lag and partition distribution
- Adjust replication factor for durability vs. performance

## Cost Optimization

- Use Spot instances for EMR workers
- Configure S3 lifecycle policies for old data
- Monitor MSK storage and throughput costs
- Implement data partitioning strategies

## Security

- IAM roles for service-to-service authentication
- VPC security groups for network isolation
- Encryption in transit and at rest
- Regular security audits and updates

## Contributing

1. Fork the repository
2. Create feature branch
3. Add tests for new functionality
4. Submit pull request with detailed description
