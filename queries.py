from dotenv import load_dotenv
from pymongo_ssh import MongoSession
import os
import json
import numpy as np

load_dotenv()

def query_1():
    pipeline = [
        {"$match": {"charge_amt_sum": {"$gt":1000000}}}
    ]

    session = MongoSession(
        host=os.getenv('HOST'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        uri="mongodb://MESIIN592023-00038:30000/"
    )

    db = session.connection["Credit"]
    provider_col = db.provider

    provider_filtered = list(provider_col.aggregate(pipeline))

    session.stop()

    return provider_filtered

def query_2():
    pipeline1 = [
        {"$match": {"city": "Beijing"}},
        {"$project": {"member_no": 1, "city": 1}},
        {"$group": { "_id": None, "member_no":{"$push":"$member_no"}}},
        {"$project": {"member_no": 1}}
    ]

    session = MongoSession(
        host=os.getenv('HOST'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        uri="mongodb://MESIIN592023-00038:30000/"
    )

    db = session.connection['Credit']
    charge_col = db.charge
    member_col = db.member

    filtered_member = list(member_col.aggregate(pipeline1))[0]['member_no']

    pipeline2 = [
        {"$match": {"$and": [{"member_no": {"$in": filtered_member}}, {"charge_amt": {"$gt": 5000}}]}},
        {"$count": "count"}
    ]

    filtered_charge = list(charge_col.aggregate(pipeline2))

    session.stop()

    return filtered_charge

def query_3():
    pipeline = [
        {"$match": {"$or": [{"region_name": {"$regex": "Europea", "$options": "i"}}, {"region_name": "Scandanavian"}]}},
        {"$project": {"_id": 0, "firstname": 1, "lastname": 1, "region_name": "$region_name"}}
    ]

    session = MongoSession(
        host=os.getenv('HOST'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        uri="mongodb://MESIIN592023-00038:30000/"
    )

    db = session.connection["Credit"]
    member_col = db.member

    member_filtered = list(member_col.aggregate(pipeline))

    session.stop()

    return member_filtered

def query_4():
    pipeline = [
        {"$match": {"num_members": {"$gte": 2}}}
    ]

    session = MongoSession(
        host=os.getenv('HOST'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        uri="mongodb://MESIIN592023-00038:30000/"
    )

    db = session.connection["Credit"]
    corporation_col = db.corporation

    corporation_filtered = list(corporation_col.aggregate(pipeline))

    session.stop()

    return corporation_filtered

def query_5():
    pipeline = [
        {"$group": {"_id": { "category_desc": "$category_desc","member_no": "$member_no"}, "max_payment": {"$max": "$charge_amt"}}},
        {"$project": {"_id": 1, "category_desc": "$_charge.category_desc", "member_no": "$_char.member_no", "max_payment": 1}}
    ]

    session = MongoSession(
        host=os.getenv('HOST'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        uri="mongodb://MESIIN592023-00038:30000/"
    )

    db = session.connection["Credit"]
    charge_col = db.charge

    charge_filtered = list(charge_col.aggregate(pipeline))

    session.stop()

    return charge_filtered

def query_6():
    pipeline=[
        {"$group" : { "_id": { "provider_no" :"$provider_no", "category_desc": "$category_desc"}, "sum_charge": {"$sum" : "$charge_amt"}}},
        {"$project": { "_id": 0, "provider_no": "$_id.provider_no", "provider_name": "$_id.provider_name", "category_desc": "$_id.category_desc", "sum_charge": 1}} 
    ]

    session = MongoSession(
        host=os.getenv('HOST'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        uri="mongodb://MESIIN592023-00038:30000/"
    )

    db = session.connection["Credit"]
    charge_col = db.charge

    charge_filtered = list(charge_col.aggregate(pipeline))

    session.stop()

    return charge_filtered

def query_7():
    pipeline = [
        {"$match": {"charges": {"$ne": None}}},
        {"$group": { "_id": "$region_name", "list_charges":{"$push":"$charges"}}},
        {"$project": {"_id": 1, "charges": {"$reduce": {"input": "$list_charges", "initialValue": [], "in": {"$concatArrays": ["$$value", "$$this"]}}}}},
        {
            "$project": {
                "region_name": "$_id",
                "charges": "$charges",
                "number_of_charges": {"$size": "$charges"},
                "mean": {"$avg": "$charges"},
                "_id": 0
            }
        }
    ]

    session = MongoSession(
        host=os.getenv('HOST'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        uri="mongodb://MESIIN592023-00038:30000/"
    )

    db = session.connection["Credit"]
    member_col = db.member

    member_filtered = list(member_col.aggregate(pipeline))

    session.stop()

    for region in member_filtered:
        region["first_quartile"] = np.percentile(np.array(region['charges']), 25)
        region["median"] = np.percentile(np.array(region['charges']), 50)
        region["third_quartile"] = np.percentile(np.array(region['charges']), 75)
        del region["charges"]

    return member_filtered

def query_8():
    pipeline=[
        {"$group" : { "_id": { "member_no" :"$member_no", "category_desc": "$category_desc"}, "avg_charge": {"$avg" : "$charge_amt"}}},
        {"$project": { "_id": 0, "member_no": "$_id.member_no", "category_desc": "$_id.category_desc", "avg_charge": 1}} 
    ]

    session = MongoSession(
        host=os.getenv('HOST'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        uri="mongodb://MESIIN592023-00038:30000/"
    )

    db = session.connection["Credit"]
    charge_col = db.charge

    charge_filtered = list(charge_col.aggregate(pipeline))

    session.stop()

    return charge_filtered

