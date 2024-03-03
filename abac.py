import json
from periodic import periodic
import re
import struct
from bson import BSON
import asyncio
# from message import generate_find_OP_MSG
operations = [ 'insert', 'find', 'update', 'delete', 'count']
user_name = 'Alice'
ip ="127.0.0.1"


# MongoDB server address and port
MONGODB_HOST = '127.0.0.1'  # localhost
MONGODB_PORT = 27017  # default MongoDB port





# Attribute based access control system (ABAC)
def abac(database, collection, operation):

    # read user attributes from './data/user_attributes.json' file
    with open('data/user_attributes.json', 'r') as json_file:
        # Load the JSON data
        data = json.load(json_file)
        if user_name in data:
            user_attrs = data[user_name]
            with open('data/object_attributes.json', 'r') as json_file:
                obj_attrs = json.load(json_file)
                with open('data/policy.json', 'r') as json_file:
                    policies = json.load(json_file)
                    allow = False
                    for policy in policies:
                        count_true = 0
                        # match user attributes
                        tc = 0
                        for req_attr in policy["user_attributes"]:
                            if policy["user_attributes"][req_attr] == user_attrs[req_attr]:
                                tc+=1
                        if tc == len(policy["user_attributes"]):
                            count_true+=1
                        # match object attributes
                        tc = 0
                        for req_attr in policy["object_attributes"]:
                            if policy["object_attributes"][req_attr] == obj_attrs[req_attr]:
                                tc+=1
                        if tc == len(policy["object_attributes"]):
                            count_true+=1
                        # match env attributes
                        ip_req = policy['env']['ip']
                        ip_found = False

                        for ip_check in ip_req:
                            ip_regex = re.compile(ip_check)
                            if ip_regex.match(ip):
                                ip_found = True
                                break
                        p = periodic(None, None, None, None, None, policy["env"]["time"])
                        if p.satisfies() and ip_found:
                            count_true+=1
                        # match permissions
                        if collection in policy["permissions"] and operation in policy["permissions"][collection]:
                            count_true+=1
                        # print(count_true)
                        if count_true == 4:
                            allow = True
                            break
                    return allow
        else:
            print("User not found")
            return False
    return True

# Check if ABAC is available
def is_abac_available(header, parsed, sender):
    if sender == 0 and header[3] == 2013:
        # Check for actions
        db = None
        collection = None
        dictionary = parsed['sections'][0]
        
        for operation in operations:
            if operation in dictionary:
                db = dictionary['$db']
                collection = dictionary[operation]
                return True, abac(db, collection, operation), db, collection, operation
                

        
    return False, None, None, None, None