import json
import requests
from influxdb import InfluxDBClient
import psycopg2
import time
from io import StringIO
import dateutil.parser
import datetime
import os

NODE_URL = 'http://nadejde-eos-node.northeurope.cloudapp.azure.com:8080'
BLOCK_PATH = '/v1/chain/get_block'
TRANSACTION_PATH = '/v1/history/get_transaction'
INFO_PATH ='/v1/chain/get_info'
INFLUX_URL = 'http://localhost:8086/write?db=eos'

WRITE_INFLUX = True
WRITE_POSTGRE = True
BATCH_SIZE = 100

#global variables to keep track of prices 
#used for buyram actions as there's no way to figure out how much ram was bought without
ramprice = 0.0
ramprice_inc_fee = 0.0

influx_client = InfluxDBClient(database='eos')
pg_config = {
    'host': os.environ['PG_EOS_HOST'],
    'dbname': os.environ['PG_EOS_DBNAME'],
    'user': os.environ['PG_EOS_USER'],
    'password': os.environ['PG_EOS_PASSWORD']
}

postgre_connection = psycopg2.connect("host=%(host)s dbname=%(dbname)s user=%(user)s password=%(password)s sslmode=verify-full sslrootcert=/home/nadejde/.postgresql/BaltimoreCyberTrustRoot.crt.pem" % pg_config)
posgre_cursor = postgre_connection.cursor()

#influx
#######
def write_points_influx_line(points):
    lines = ""
    for point in points:
        line_patern = "ram,action=%(action)s,payer=%(payer)s,receiver=%(receiver)s block_num=%(block_num)si,bytes=%(bytes)s,fee=%(fee)s,global_sequence=%(global_sequence)si,ramprice=%(ramprice)s,ramprice_inc_fee=%(ramprice_inc_fee)s,tokens=%(tokens)s,trx=\"%(trx)s\" %(timestamp)s\n"
        point['timestamp'] = int(time.mktime(dateutil.parser.parse(point['time']).timetuple()) * 1000 * 1000000)
        lines = lines + (line_patern % point)
        
    influx_client.write_points(lines, protocol = 'line')

def write_points_influx(points):
    json_body = []
    for point in points:
        json_body.append(
            {
                'measurement': 'ram',
                'tags': {
                    'action': point['action'],
                    'receiver': point['receiver'],
                    'payer': point['payer']
                },
                'time': point['time'],
                'fields': {
                    'ramprice_inc_fee': point['ramprice_inc_fee'],
                    'tokens': point['tokens'],
                    'ramprice': point['ramprice'],
                    'bytes': point['bytes'],
                    'fee': point['fee'],
                    'trx': point['trx'],
                    'block_num': point['block_num'],
                    'global_sequence': point['global_sequence']
                }
            }
        )
    influx_client.write_points(json_body)
 
#postgre
########
def write_points_postgre(points):
    #building file like object to use with postgre copy comand (real fast insert:)
    points_string = ''
    for point in points:
        p_string = '%(action)s\t%(receiver)s\t%(payer)s\t%(time)s\t%(ramprice_inc_fee)s\t%(tokens)s\t%(ramprice)s\t%(bytes)s\t%(fee)s\t%(trx)s\t%(block_num)s\t%(global_sequence)s\n'
        points_string = points_string + (p_string % point)

    f = StringIO(points_string)
    posgre_cursor.copy_from(f, 'ram', columns=('action', 'receiver', 'payer', 'time', 'ramprice_inc_fee', 'tokens', 'ramprice', 'bytes', 'fee', 'trx', 'block_num', 'global_sequence'))
    postgre_connection.commit()

def last_block_and_price_db():
    posgre_cursor.execute("select block_num, ramprice, ramprice_inc_fee from ram order by block_num desc fetch first 1 rows only;")
    return posgre_cursor.fetchone()

    
#nodeos
#######
#calls EOS node and retrieves a transaction by id
def get_transaction(tx_id):
    data = {
        'id': tx_id
    }
    response = requests.post(NODE_URL + TRANSACTION_PATH, json=data)
    response.raise_for_status()

    return response.json()
    
#calls EOS node and retrieves a block by id
def get_block(block_num_or_id):
    data = {
        'block_num_or_id': block_num_or_id
    }
    response = requests.post(NODE_URL + BLOCK_PATH, json=data)
    response.raise_for_status()

    return response.json()

def get_last_chain_block():
    response = requests.post(NODE_URL + INFO_PATH)
    response.raise_for_status()
    json = response.json()
    return json['head_block_num']

################

def write_points(points):
    if WRITE_INFLUX:
        write_points_influx_line(points)
    if WRITE_POSTGRE:
        write_points_postgre(points)
   

def check_for_ram_act(actions):
    for action in actions:
        if(action['name'] == 'buyrambytes' or action['name'] == 'buyram'  or 
                action['name'] == 'sellrambytes' or action['name'] == 'sellram'):
            return True
    return False
 
#retrieve all transaction ids from one block    
def get_block_ram_transactions(block):
    #print('block: ' + str(block['block_num']) + ' - tx count: ' + str(len(block['transactions'])))
    transactions = []
    for transaction in block['transactions']:
        if (transaction['status'] == 'executed' and
                'transaction' in transaction['trx'] and
                len(transaction['trx']['transaction']['actions']) > 0):
            if (check_for_ram_act(transaction['trx']['transaction']['actions'])):
                transactions.append(transaction['trx']['id'])
    return transactions
    
#helper to retrieve tokens, fee and bytes from a trace (part of action)
def get_tokens_bytes(trace):
    tokens = (trace['inline_traces'][0]['act']['data']['quantity'].split())[0]
    fee = (trace['inline_traces'][1]['act']['data']['quantity'].split())[0] if len(trace['inline_traces']) > 1 else '0.0'
    bytes = trace['act']['data']['bytes']
    
    return tokens, fee, bytes
 
#processes a trace and builds a point   
def process_trace(trace):
    global ramprice
    global ramprice_inc_fee
    global_sequence = trace['receipt']['global_sequence']
    
    if len(trace['inline_traces']) == 0:
        return None
    
    if trace['act']['name'] == 'buyrambytes':
        tokens, fee, bytes = get_tokens_bytes(trace)
        #ramprice gets calculated based on spent tokens and bytes
        ramprice = float(tokens) / float(bytes)
        ramprice_inc_fee = (float(tokens) + float(fee)) / float(bytes)
        
        return {
            'action': 'buyrambytes',
            'payer': trace['act']['data']['payer'], #who payed for ram
            'receiver': trace['act']['data']['receiver'], #who gets the ram
            'bytes': float(bytes), #bytes bought
            'tokens': float(tokens), #tokens payed for ram
            'fee': float(fee), #tokens payed as fee
            'ramprice': ramprice, #calculated ram price sans fee
            'ramprice_inc_fee': ramprice_inc_fee, #calculated ram price including fee
            'global_sequence': global_sequence #unique action identifier
        }
    elif trace['act']['name'] == 'sellram':    
        tokens, fee, bytes = get_tokens_bytes(trace)
        return {
            'action': 'sellram',
            'payer': trace['act']['data']['account'],
            'receiver': trace['act']['data']['account'],
            'bytes': float(-1 * bytes),
            'tokens': float('-' + tokens),
            'fee': float(fee),
            'ramprice': float(tokens) / float(bytes),
            'ramprice_inc_fee': (float(tokens) + float(fee)) / float(bytes),
            'global_sequence': global_sequence
        }
    elif trace['act']['name'] == 'buyram':    
        #print(trace)
        tokens = (trace['inline_traces'][0]['act']['data']['quantity'].split())[0]
        fee = (trace['inline_traces'][1]['act']['data']['quantity'].split())[0] if len(trace['inline_traces']) > 1 else '0.0'
        bytes = str((float(tokens) + float(fee)) / float(ramprice))
        return {
            'action': 'buyram',
            'payer': trace['act']['data']['payer'],
            'receiver': trace['act']['data']['receiver'],
            'bytes': float(bytes),
            'tokens': float(tokens),
            'fee': float(fee),
            'ramprice': ramprice,
            'ramprice_inc_fee': ramprice_inc_fee,
            'global_sequence': global_sequence
        }
        
    return None    
    
def process_blocks(start_block, end_block):
    points = []
    for block_num in range(start_block, end_block + 1):
        block = get_block(block_num)
        transactions = get_block_ram_transactions(block)
        for transaction in transactions:
            tx_body = get_transaction(transaction)
            for trace in tx_body['traces']:
                point = process_trace(trace)
                if point:
                    point['block_num'] = tx_body['block_num']
                    point['trx'] = tx_body['id']
                    point['time'] = tx_body['block_time']
                    points.append(point)
    
        if(block_num % BATCH_SIZE == 0):
            write_points(points) #writing BATCH_SIZE blocks worth of actions at a time more efficient
            points = []
            
    if len(points) > 0:
        write_points(points) #write any remaning ponints out.
 

def grabber(): 
    global ramprice
    global ramprice_inc_fee
    last_db_block, ramprice, ramprice_inc_fee = last_block_and_price_db() #when grabber starts up get last inseted block
    
    while True:
        start = time.time()
        last_chain_block = get_last_chain_block()
        process_blocks(last_db_block + 1, last_chain_block)
        end = time.time()
        print(str(last_chain_block - last_db_block) + ' blocks in ' + str(end - start) + ' sec' + ' last block: ' + str(last_chain_block))
        last_db_block = last_chain_block #move to next batch of blocks
        time.sleep(10)

grabber()

posgre_cursor.close()
postgre_connection.close()