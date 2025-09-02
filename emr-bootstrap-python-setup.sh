#!/bin/bash
sudo yum update -y
sudo yum install -y python3-pip

# Install required Python packages
sudo pip3 install kafka-python==2.0.2 boto3 aws-msk-iam-sasl-signer-python

# Download application code
aws s3 cp s3://<s3_bucket>/ecommerce_streaming.py /home/hadoop/ # Replace with actual S3 bucket
chmod +x /home/hadoop/ecommerce_streaming.py