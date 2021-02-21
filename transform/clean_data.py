'''Provides functions to convert raw transactions to clean transactions and baskets.
'''

def clean_transactions(raw_transaction_list):
    '''Clean list of raw transaction dictionaries.'''
    clean_transaction_list = []
    for raw_transaction in raw_transaction_list:
        # Convert datetime to 'month-day-year hh:ss' format'
        date, time = raw_transaction['date'].split(maxsplit=1)
        day, month, year = date.split('/')
        clean_date = f'{month}-{day}-{year}'
        # Create unique ID
        abbreviated_location = raw_transaction['location'][:4].upper()
        unique_key = raw_transaction['identity']
        id_string = f'{clean_date}-{abbreviated_location}-{unique_key}-{raw_transaction["id_number"]}'
        # Remove surname
        name = raw_transaction['customer_name'].split()[0]
        # Create clean transaction and append to list
        clean_transaction = {
            'unique_id': id_string,
            'date': clean_date,
            'time': time,
            'location': raw_transaction['location'],
            'first_name': name,
            'total': float(raw_transaction['pay_amount']),
            'payment_method': raw_transaction['payment_method'],
            'basket': raw_transaction['basket'],
            'date_time': f"{clean_date} {time}"
        }
        clean_transaction_list.append(clean_transaction)
    return clean_transaction_list

def create_baskets(clean_transactions_list):
    '''Returns a list of basket dictionaries from a list of clean transactions.
    '''
    basket_list = []
    for transaction in clean_transactions_list:
        transaction_id = transaction['unique_id']
        basket = transaction['basket']
        # Split basket into into individual items
        items = basket.strip('"').split(', ')
        for item in items:
            item, cost = item.rsplit(' - ', 1)
            product, flavour, size = split_basket_item(item)
            basket_list.append({
                'trans_id': transaction_id,
                'item': product,
                'flavour': flavour,
                'size': size,
                'cost': float(cost)
            })
    return basket_list

def split_basket_item(basket_item):
    '''Splits basket item into [product, flavour, size]
    
    'Large Flavoured latte - Hazelnut' --> ('Flavoured latte', 'Hazelnut', 'large')
    'Cortado' --> ('Cortado', 'standard', 'standard')
    '''
    # Get product size
    if basket_item.startswith('Large'):
        size = 'large'
        basket_item = basket_item[6:]
    elif basket_item.startswith('Regular'):
        size = 'regular'
        basket_item = basket_item[8:]
    else:
        size = 'standard'
    # Get product flavour (if given)
    try:
        product, flavour = basket_item.split(' - ')
    except ValueError:
        product = basket_item
        flavour = 'standard'
    return [product, flavour, size]