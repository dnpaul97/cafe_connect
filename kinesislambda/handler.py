import json
import logging

import boto3

from sqs_messaging import send_sqs_message

logging.getLogger().setLevel(logging.INFO)

def start(event, context):
    # Get data stream
    kinesis_stream = event['Records']
    counter = 0
    for data_record in kinesis_stream:
        # Convert binary string to python list
        row_binary = data_record['Data']
        logging.info({'binary row': row_binary})
        row_string = row_binary.decode('utf-8')
        logging.info({'decoded row': row_string})
        row = json.loads(row_string)
        logging.info({'row': row})
        # Create unique identifier
        identifier = row[1].ljust(12, 'a')
        # Convert list to dictionary
        raw_transaction = {
            'date': row[0],
            'location': row[1],
            'customer_name': row[2],
            'basket': row[3],
            'pay_amount': row[4],
            'payment_method': row[5],
            'ccn': row[6],
            'id_number': counter,
            'identity': identifier
        }
        logging.info({'raw transaction': raw_transaction})
        counter += 1
        # Convert dictionary to json list
        message = json.dumps([raw_transaction])
        # Send row SQS
        queue_name = 'Group1SQSKinesistoTransorm'
        queue_url = 'https://sqs.eu-west-1.amazonaws.com/579154747729/Group1SQSKinesistoTransorm'
        try:
            send_sqs_message(
                message,
                queue_name=queue_name,
                queue_url=queue_url
            )
            logging.info({'message': message})
        except Exception as ERROR:
            logging.error({'message failed': message})
            logging.error(ERROR)
