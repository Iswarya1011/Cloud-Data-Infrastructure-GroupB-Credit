import pymongo
from dotenv import load_dotenv
from pymongo_ssh import MongoSession
from sshtunnel import SSHTunnelForwarder
import os

load_dotenv()

# define ssh tunnel
# server = SSHTunnelForwarder(
#     os.getenv('HOST'),
#     ssh_username=os.getenv('USER'),
#     ssh_password=os.getenv('PASSWORD'),
#     remote_bind_address=('127.0.0.1', 27017)
# )

# server.start()

# connection = pymongo.MongoClient('127.0.0.1', server.local_bind_port)
# print(connection.list_database_names())

# db = connection["test"]

# print(db.list_collection_names())

# connection.close()
# server.stop()

session = MongoSession(
    host=os.getenv('HOST'),
    user=os.getenv('USER'),
    password=os.getenv('PASSWORD'),
    uri="mongodb://MESIIN592023-00039:27017/"
)

db = session.connection['test']
print(db.list_collection_names())

session.close()