from dotenv import load_dotenv
from pymongo_ssh import MongoSession
import os
import numpy as np
import timeit
import datetime
import json
from tqdm import tqdm

load_dotenv()


def get_shard_number():
    session = MongoSession(
        host=os.getenv("HOST"),
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD"),
        uri="mongodb://MESIIN592023-00038:30000/",
    )

    db = session.connection["config"]
    shards_col = db.shards

    shards = list(shards_col.find())

    session.stop()
    return shards


def query_1():
    pipeline = [{"$match": {"charge_amt_sum": {"$gt": 1000000}}}]

    session = MongoSession(
        host=os.getenv("HOST"),
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD"),
        uri="mongodb://MESIIN592023-00038:30000/",
    )

    db = session.connection["Credit"]
    provider_col = db.provider

    provider_filtered = list(provider_col.aggregate(pipeline))

    session.stop()

    # print("Done")

    return provider_filtered


def query_2():
    pipeline1 = [
        {"$match": {"city": "Beijing"}},
        {"$project": {"member_no": 1, "city": 1}},
        {"$group": {"_id": None, "member_no": {"$push": "$member_no"}}},
        {"$project": {"member_no": 1}},
    ]

    session = MongoSession(
        host=os.getenv("HOST"),
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD"),
        uri="mongodb://MESIIN592023-00038:30000/",
    )

    db = session.connection["Credit"]
    charge_col = db.charge
    member_col = db.member

    filtered_member = list(member_col.aggregate(pipeline1))[0]["member_no"]

    pipeline2 = [
        {
            "$match": {
                "$and": [
                    {"member_no": {"$in": filtered_member}},
                    {"charge_amt": {"$gt": 5000}},
                ]
            }
        },
        {"$count": "count"},
    ]

    filtered_charge = list(charge_col.aggregate(pipeline2))

    session.stop()

    return filtered_charge


def query_3():
    pipeline = [
        {
            "$match": {
                "$or": [
                    {"region_name": {"$regex": "Europea", "$options": "i"}},
                    {"region_name": "Scandanavian"},
                ]
            }
        },
        {
            "$project": {
                "_id": 0,
                "firstname": 1,
                "lastname": 1,
                "region_name": "$region_name",
            }
        },
    ]

    session = MongoSession(
        host=os.getenv("HOST"),
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD"),
        uri="mongodb://MESIIN592023-00038:30000/",
    )

    db = session.connection["Credit"]
    member_col = db.member

    member_filtered = list(member_col.aggregate(pipeline))

    session.stop()

    return member_filtered


def query_4():
    pipeline = [{"$match": {"num_members": {"$gte": 2}}}]

    session = MongoSession(
        host=os.getenv("HOST"),
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD"),
        uri="mongodb://MESIIN592023-00038:30000/",
    )

    db = session.connection["Credit"]
    corporation_col = db.corporation

    corporation_filtered = list(corporation_col.aggregate(pipeline))

    session.stop()

    return corporation_filtered


def query_5():
    pipeline = [
        {
            "$group": {
                "_id": {"category_desc": "$category_desc", "member_no": "$member_no"},
                "max_payment": {"$max": "$charge_amt"},
            }
        },
        {
            "$project": {
                "_id": 1,
                "category_desc": "$_charge.category_desc",
                "member_no": "$_char.member_no",
                "max_payment": 1,
            }
        },
    ]

    session = MongoSession(
        host=os.getenv("HOST"),
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD"),
        uri="mongodb://MESIIN592023-00038:30000/",
    )

    db = session.connection["Credit"]
    charge_col = db.charge

    charge_filtered = list(charge_col.aggregate(pipeline))

    session.stop()

    return charge_filtered


def query_6():
    pipeline = [
        {
            "$group": {
                "_id": {
                    "provider_no": "$provider_no",
                    "category_desc": "$category_desc",
                },
                "sum_charge": {"$sum": "$charge_amt"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "provider_no": "$_id.provider_no",
                "provider_name": "$_id.provider_name",
                "category_desc": "$_id.category_desc",
                "sum_charge": 1,
            }
        },
    ]

    session = MongoSession(
        host=os.getenv("HOST"),
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD"),
        uri="mongodb://MESIIN592023-00038:30000/",
    )

    db = session.connection["Credit"]
    charge_col = db.charge

    charge_filtered = list(charge_col.aggregate(pipeline))

    session.stop()

    return charge_filtered


def query_7():
    pipeline = [
        {"$match": {"charges": {"$ne": None}}},
        {"$group": {"_id": "$region_name", "list_charges": {"$push": "$charges"}}},
        {
            "$project": {
                "_id": 1,
                "charges": {
                    "$reduce": {
                        "input": "$list_charges",
                        "initialValue": [],
                        "in": {"$concatArrays": ["$$value", "$$this"]},
                    }
                },
            }
        },
        {
            "$project": {
                "region_name": "$_id",
                "charges": "$charges",
                "number_of_charges": {"$size": "$charges"},
                "mean": {"$avg": "$charges"},
                "_id": 0,
            }
        },
    ]

    session = MongoSession(
        host=os.getenv("HOST"),
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD"),
        uri="mongodb://MESIIN592023-00038:30000/",
    )

    db = session.connection["Credit"]
    member_col = db.member

    member_filtered = list(member_col.aggregate(pipeline))

    session.stop()

    for region in member_filtered:
        region["first_quartile"] = np.percentile(np.array(region["charges"]), 25)
        region["median"] = np.percentile(np.array(region["charges"]), 50)
        region["third_quartile"] = np.percentile(np.array(region["charges"]), 75)
        del region["charges"]

    return member_filtered


def query_8():
    pipeline = [
        {
            "$group": {
                "_id": {"member_no": "$member_no", "category_desc": "$category_desc"},
                "avg_charge": {"$avg": "$charge_amt"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "member_no": "$_id.member_no",
                "category_desc": "$_id.category_desc",
                "avg_charge": 1,
            }
        },
    ]

    session = MongoSession(
        host=os.getenv("HOST"),
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD"),
        uri="mongodb://MESIIN592023-00038:30000/",
    )

    db = session.connection["Credit"]
    charge_col = db.charge

    charge_filtered = list(charge_col.aggregate(pipeline))

    session.stop()

    return charge_filtered

def get_time(func):

    final_dict = {}

    final_dict["date"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
    final_dict["func_name"] = func.__name__
    final_dict["shard_nb"] = len(get_shard_number())
    execution_time = timeit.repeat(func, repeat=10, number=1)
    execution_time2 = execution_time.copy()
    # execution_time = timeit.timeit(func, number=1)
    final_dict["execution_time_list"] = execution_time

    execution_time2.remove(max(execution_time2))
    execution_time2.remove(min(execution_time2))

    final_dict["execution_time_list_2"] = execution_time2

    final_dict["time_avg"] = sum(execution_time2) / len(execution_time2)

    return final_dict

func_list = [query_1, query_2, query_3, query_4, query_5, query_6, query_7, query_8]


def measure_query_execution(func_list: list):
    shard_nb = len(get_shard_number())
    query_measures = []
    for func in tqdm(func_list):
        query_measures.append(get_time(func))

    with open(f"time_measures/query_time_measure_shard_{shard_nb}.json", "w") as f:
        json.dump(query_measures, f, indent=3)
    
    return query_measures

print(measure_query_execution(func_list))