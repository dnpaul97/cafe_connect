import json
import logging

import boto3
import psycopg2.extras
from connect_to_redshift import connect_to_redshift

logging.getLogger().setLevel(logging.ERROR)

def start(event, context):
    # Read data from SQS
    json_data = event['Records'][0]['body']
    data = json.loads(json_data)
    # Check if baskets or transactions data
    if 'transactions' in data:
        data_is_transactions = True
        transaction_list = data['transactions']
        logging.info({'Transaction list': {
            'first_row': transaction_list[0]
        }})
    else:
        data_is_transactions = False
        basket_list = data['baskets']
        logging.info({'Basket list':{
            'first_row': basket_list[0]
        }})

    # Connect to redshift
    conn = connect_to_redshift()

    # Write data to redshift
    if data_is_transactions:
        # If SQS message contained transactions, write transactions...
        with conn.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, """
                INSERT INTO transactions_g3 VALUES %s;
            """, [(
                transaction['unique_id'],
                transaction['date'],
                transaction['time'],
                transaction['location'],
                transaction['first_name'],
                transaction['total'],
                transaction['payment_method'],
                transaction['date_time']
            ) for transaction in transaction_list])
            conn.commit()
            logging.info('Transactions written to database')
    else:
        # If SQS message contained baskets, write baskets...
        with conn.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, """
                INSERT INTO basket_g3 (trans_id, product, flavour, size, cost) VALUES %s;
            """, [(
                basket['trans_id'],
                basket['item'],
                basket['flavour'],
                basket['size'],
                basket['cost']
            ) for basket in basket_list])
            conn.commit()
            logging.info('Baskets written to database')
    conn.close()
    logging.info('Closed connection')
