import sys
import pymongo
import certifi

from US_VISA.exception import USvisaException
from US_VISA.logger import logging
from US_VISA.constants import DATABASE_NAME, MONGOBD_URL_KEY

ca = certifi.where()

class MongoDBClient:
    """
    Creates and manages MongoDB connection
    """

    client = None

    def __init__(self, database_name: str = DATABASE_NAME):
        try:
            if MongoDBClient.client is None:
                MongoDBClient.client = pymongo.MongoClient(
                    MONGOBD_URL_KEY,
                    tlsCAFile=ca
                )

            self.client = MongoDBClient.client
            self.database = self.client[database_name]
            self.database_name = database_name

            logging.info(f"MongoDB connected successfully to DB: {database_name}")

        except Exception as e:
            raise USvisaException(e, sys)
