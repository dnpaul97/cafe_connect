'''Clean data from extract module and send to load module using SQS

1. Receive a JSON list of raw transactions from SQS
2. Convert to a list of clean transactions and a list of baskets
3. Send each list in an SQS message
'''
import json, logging, os

import boto3
import json

from clean_data import clean_transactions, create_baskets
from sqs_messaging import send_message_list_to_sqs, split_long_list

logging.getLogger().setLevel(logging.ERROR)

def start(event, context):
    # Read message from SQS (list raw transactions)
    logging.info('Transform lambda start')
    raw_transactions_string = event['Records'][0]['body']
    raw_transactions = json.loads(raw_transactions_string)
    logging.info('Read raw transactions data')
    
    # # Clean data
    try:
        clean_transaction_list = clean_transactions(raw_transactions)
    except Exception as ERROR:
        logging.error({'Failed to clean transactions': {
            'error': str(ERROR),
            'raw_transactions': raw_transactions
        }})
        return
    
    logging.info('Cleaned transactions')
    try:
        basket_list = create_baskets(clean_transaction_list)
    except Exception as ERROR:
        logging.error({'Failed to create baskets': {
            'error': str(ERROR),
            'raw_transactions': raw_transactions
        }})
        return
    logging.info('Created baskets')

    # Split data into smaller chunks
    transaction_chunks = split_long_list(clean_transaction_list, max_length=750)
    logging.info(f'Split transactions into {len(transaction_chunks)} chunk(s)')
    basket_chunks = split_long_list(basket_list, max_length=750)
    logging.info(f'Split baskets into {len(basket_chunks)} chunk(s)')

    # Convert data chunks to JSON strings
    transaction_messages = [json.dumps({'transactions': chunk})
                            for chunk in transaction_chunks]
    basket_messages = [json.dumps({'baskets': chunk})
                       for chunk in basket_chunks]

    # Send SQS messages
    queue_name = 'Group1SQSTransformtoLoad'
    queue_url = 'https://sqs.eu-west-1.amazonaws.com/579154747729/Group1SQSTransformtoLoad'
    
    logging.info('Sending transaction messages...')
    send_message_list_to_sqs(transaction_messages,
                             queue_name=queue_name, queue_url=queue_url)
    logging.info('Sending basket messages...')
    send_message_list_to_sqs(basket_messages,
                             queue_name=queue_name, queue_url=queue_url)
