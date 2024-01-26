from pymongo import MongoClient
from datetime import datetime

# Connection to local MongoDB
client = MongoClient("localhost", 27017)

# Your desired database name
database_name = "projectDB"
db = client[database_name]

# Collection for Tables
tables_collection = db["tables"]
tables_collection.create_index([("finalProject", 1)], unique=True)

# Collection for Users
users_collection = db["users"]
users_collection.create_index([("userId", 1)], unique=True)

# Assuming you have a unique index on userId for users
# This unique index is used to prevent duplicate users with the same ID

print("Local Database and Collections created successfully.")