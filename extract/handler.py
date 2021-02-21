import json
import logging
import os
import sys

import boto3

from read_from_s3 import (get_file_names, get_key_suffix,
                          output_raw_transactions, read_csv_file_from_s3)
from sqs_messaging import send_message_list_to_sqs, split_long_list

logging.getLogger().setLevel(logging.ERROR)

def start(event, context):
    try:
        all_files = get_file_names()
    except Exception as ERROR:
        logging.error({'S3 connection failed': str(ERROR)})
    for file_name in all_files:
        logging.info({'Extract start': file_name})
        try:
            # Read data
            data = read_csv_file_from_s3(bucket="cafe-transactions-group-1", key=file_name)
        except Exception as ERROR:
            log_message = {'CSV read failed': {
                'file_name': file_name,
                'error': str(ERROR)
            }}
            logging.error(log_message)
            continue
        logging.info({'Read CSV': file_name})
        identifier = get_key_suffix(file_name)
        logging.info({'Got identifier': file_name})
        raw_transactions = output_raw_transactions(data, identifier)
        logging.info({'Read raw transactions': file_name})
        # Split data into smaller chunks to send on SQS
        raw_transaction_chunks = split_long_list(raw_transactions, max_length=750)
        logging.info({f'Split into {len(raw_transaction_chunks)} chunk(s)': file_name})
        message_list = [json.dumps(chunk) for chunk in raw_transaction_chunks]
        logging.info({'Converted to JSON': file_name})

        # Send each json data chunk to SQS
        queue_name = 'Group1SQSExtracttoTransform'
        queue_url = 'https://sqs.eu-west-1.amazonaws.com/579154747729/Group1SQSExtracttoTransform'
        send_message_list_to_sqs(message_list,
                                 queue_name=queue_name,
                                 queue_url=queue_url,
                                 file_name=file_name)
