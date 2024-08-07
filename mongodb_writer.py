import logging
from os import environ
from pymongo import MongoClient
import time

logging.basicConfig(level=logging.INFO)

db_config = {
    'PASS': environ.get('PASS', ''),
    'DOMAIN': environ.get('DOMAIN', ''),
    'DATA_TYPE': environ.get('DATA_TYPE', ''),
    'DATA_SIZE': int(environ.get('HASH_SIZE', '')),
    'RECORDS': int(environ.get('RECORDS', '')),
    'INSERT_DELAY': int(environ.get('INSERT_DELAY', '')),
}

mongo_client = MongoClient(
    f'mongodb://root:{db_config["PASS"]}@{db_config["DOMAIN"]}:27017/',
    serverSelectionTimeoutMS=5000)


def generate_random_hash(numb: int) -> str:
    import random
    import string
    import hashlib
    return ''.join(
        [hashlib.sha256(''.join(random.choices(string.ascii_letters + string.digits, k=64)).encode()).hexdigest()
         for _ in range(numb)])


def generate_text(numb: int) -> str:
    text = ' The quick brown fox jumps over the lazy dog today. '
    return ''.join([text for _ in range(numb)])


def mongo_write(size: int = 100) -> bool:
    if not mongo_client.server_info():
        logging.error("Failed to connect to the database.")
        return False

    logging.info("Connected to the database.")
    db = mongo_client.db
    if db_config['DATA_TYPE'] == 'h':
        table_name = 'hashes'
        func = generate_random_hash
    elif db_config['DATA_TYPE'] == 't':
        table_name = 'texts'
        func = generate_text
    else:
        logging.error("Invalid data type.")
        return False

    try:
        for _ in range(size):
            time.sleep(db_config['INSERT_DELAY'] * 0.001)
            db.hashes.insert_one({f'{table_name}': func(db_config['DATA_SIZE'])})
    except Exception as e:
        logging.error(f"Failed to write data to the database: {e}")
        return False

    return True


if __name__ == '__main__':
    if mongo_write(db_config['RECORDS']):
        logging.info("Data successfully written to the database.")
    else:
        logging.error("Failed to write hashes to the database.")
