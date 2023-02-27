import pymongo

from transpose import Transpose

from os import getenv
from dotenv import load_dotenv


load_dotenv()


def azuki_metadata():
    """Returns a collection from the MongoDB database."""
    client = pymongo.MongoClient(getenv('MONGO'))
    db = client.azuki
    return db.metadata

def azuki_sales():
    """Returns a collection from the MongoDB database."""
    client = pymongo.MongoClient(getenv('MONGO'))
    db = client.azuki
    return db.sales

def transpose_connect():
    api = Transpose(getenv('TRANSPOSE'))
    return api