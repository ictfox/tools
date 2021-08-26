#!/usr/bin/env python
# coding=utf8

from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client
from qcloud_cos import CosServiceError
from qcloud_cos import CosClientError
from tencentcloud.common import credential
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.cam.v20190116 import cam_client, models

import ast
import re
import os
import sys
import time
import json
import datetime
import logging

os.environ['TZ'] = 'Asia/Shanghai'
#logging.basicConfig(level=logging.INFO, stream=sys.stdout)
#logger = logging.getLogger()

# Config with your own secretid/secretkey
secret_id = 'your secret id'
secret_key = 'your secret key'

cos_buckets = []
cos_clients = {}
cam_users = []
cam_groups = []
cam_roles = []

def is_digit(str):
    try:
        float(str)
    except ValueError:
        return False
    return True

def is_cos_bucket_style(str):
    dlist = str.split('-')
    return is_digit(dlist[-1])

def json_dump_object(obj, indent=2):
    return json.dumps(obj, sort_keys=True, indent=indent,
                      separators=(',', ' : '), ensure_ascii=False)

class CosBucket(object):
    def __init__(self, creationdate, name, location, client):
        self.creationdate = creationdate
        self.name = name
        self.location = location
        self.client = client
        self.acl = None
        self.policy = None

    def __str__(self):
        return 'CosBucket: {\n' \
               '  Name: %s,\n' \
               '  Location: %s,\n' \
               '  CreationDate: %s\n' \
               '  ACL: %s\n' \
               '  Policy: %s\n' \
               '}' % \
               (self.name, self.location, self.creationdate,
                json_dump_object(self.acl), json_dump_object(self.policy))

    def get_name(self):
        return self.name

    def get_region(self):
        return self.region

    def get_acl(self):
        return self.acl

    def get_policy(self):
        return self.policy

    def set_acl(self):
        try:
            resp = self.client.get_bucket_acl(Bucket=self.name)
            self.acl = resp['AccessControlList']['Grant']
        except CosServiceError as err:
            print(err)

    def set_policy(self):
        try:
            resp = self.client.get_bucket_policy(Bucket=self.name)
            self.policy = ast.literal_eval(resp['Policy'])
        except CosServiceError as err:
            print(err)

class CamUser(object):
    def __init__(self, name, uid, uin, consolelogin, remark, client):
        self.name = name
        self.uid = uid
        self.uin = uin
        self.consolelogin = consolelogin
        self.remark = remark
        self.client = client
        self.policy = None

    def __str__(self):
        return 'CamUser: {\n' \
               '  Name: %s,\n' \
               '  Uid: %s,\n' \
               '  Uin: %s\n' \
               '  ConsoleLogin: %s\n' \
               '  Remark: %s\n' \
               '  CosPolicy: %s\n' \
               '}' % \
               (self.name, self.uid, self.uin, self.consolelogin, self.remark,
                json_dump_object(self.policy))

    def dict(self):
        return {"Name": self.name, "Uid": self.uid,
                "Uin": self.uin, "ConsoleLogin": self.consolelogin,
                "Remark": self.remark, "CosPolicy": self.policy}

    def get_uin(self):
        return self.uin

    def set_policy(self):
        Page = 1
        Rp = 100
        self.policy = []
        while True:
            req = models.ListAttachedUserPoliciesRequest()
            params = '''{
                "TargetUin": ''' + str(self.uin) + ''',
                "Page": ''' + str(Page) + ''',
                "Rp": ''' + str(Rp) + '''
            }'''
            req.from_json_string(params)
            resp = self.client.ListAttachedUserPolicies(req)
            for p in resp.List:
                str_p = str(p).lower()
                if str_p.find("cos") > 0:
                    self.policy.append({"PolicyName":p.PolicyName,
                                        "Remark":p.Remark, "PolicyType":p.PolicyType})

            if Page * Rp >= resp.TotalNum: break
            Page = Page + 1

class CamGroup(object):
    def __init__(self, name, id, remark, createtime, client):
        self.name = name
        self.id = id
        self.remark = remark
        self.createtime = createtime
        self.client = client
        self.users = None
        self.policy = None

    def __str__(self):
        return 'CamGroup: {\n' \
               '  Name: %s,\n' \
               '  ID: %s,\n' \
               '  Remark: %s\n' \
               '  CreateTime: %s\n' \
               '  UserInfo: %s\n' \
               '  Policy: %s\n' \
               '}' % \
               (self.name, self.id, self.remark,
                self.createtime, self.users, self.policy)

    def get_name(self):
        return self.name

    def get_users(self):
        return self.users

    def get_policy(self):
        return self.policy

    def set_users(self):
        req = models.GetGroupRequest()
        params = '''{
            "GroupId": ''' + str(self.id) + '''
        }'''
        req.from_json_string(params)
        resp = self.client.GetGroup(req)
        self.users = resp.UserInfo

    def set_policy(self):
        Page = 1
        Rp = 100
        while True:
            req = models.ListAttachedGroupPoliciesRequest()
            params = '''{
                "TargetGroupId": ''' + str(self.id) + ''',
                "Page": ''' + str(Page) + ''',
                "Rp": ''' + str(Rp) + '''
            }'''
            req.from_json_string(params)
            resp = self.client.ListAttachedGroupPolicies(req)
            self.policy = []
            for p in resp.List:
                self.policy.append({"PolicyName":p.PolicyName,
                                    "Remark":p.Remark, "PolicyType":p.PolicyType})

            if Page * Rp >= resp.TotalNum: break
            Page = Page + 1

class CamRole(object):
    def __init__(self, name, id, type, description, policy_doc, client):
        self.name = name
        self.id = id
        self.type = type
        self.description = description
        self.policy_doc = ast.literal_eval(policy_doc)['statement']
        self.client = client
        self.policy = None

    def __str__(self):
        return 'CamRole: {\n' \
               '  Name: %s,\n' \
               '  ID: %s,\n' \
               '  Type: %s\n' \
               '  Description: %s\n' \
               '  PolicyDoc: %s\n' \
               '  Policy: %s\n' \
               '}' % \
               (self.name, self.id, self.type, self.description,
                json_dump_object(self.policy_doc), json_dump_object(self.policy))

    def dict(self):
        return {"Name": self.name, "ID": self.id,
                "Type": self.type, "description": self.description,
                "policy document":self.policy_doc, "policy": self.policy}

    def set_policy(self):
        Page = 1
        Rp = 100
        while True:
            req = models.ListAttachedRolePoliciesRequest()
            params = '''{
                "RoleId": "''' + str(self.id) + '''",
                "Page": ''' + str(Page) + ''',
                "Rp": ''' + str(Rp) + '''
            }'''
            req.from_json_string(params)
            resp = self.client.ListAttachedRolePolicies(req)
            self.policy = []
            for p in resp.List:
                self.policy.append({"PolicyName":p.PolicyName, "Description":p.Description,
                                    "PolicyId":p.PolicyId, "PolicyType":p.PolicyType})

            if Page * Rp >= resp.TotalNum: break
            Page = Page + 1

def print_buckets():
    for bucket in cos_buckets:
        print(bucket)

def print_users():
    for user in cam_users:
        print(user)

def print_groups():
    for group in cam_groups:
        print(group)

def print_roles():
    for role in cam_roles:
        print("CamRole: ", json_dump_object(role.dict()))

def get_buckets():
    config = CosConfig(Secret_id=secret_id, Secret_key=secret_key, Region='ap-beijing')
    client = CosS3Client(config)
    resp = client.list_buckets()
    buckets = resp['Buckets']
    return buckets['Bucket']

def init_cos_client(region):
    global cos_clients
    if region in cos_clients: 
        return cos_clients[region]

    config = CosConfig(Secret_id=secret_id, Secret_key=secret_key, Region=region)
    client = CosS3Client(config)
    cos_clients['region'] = client
    return client

def list_users(client):
    global cam_users
    if len(cam_users) > 0: return

    req = models.ListUsersRequest()
    resp = client.ListUsers(req)
    users = resp.Data
    for user in users:
        cam_user = CamUser(user.Name, user.Uid, user.Uin,
                           user.ConsoleLogin, user.Remark, client)
        cam_user.set_policy()
        cam_users.append(cam_user)

def list_groups(client):
    global cam_groups
    if len(cam_groups) > 0: return

    Page = 1
    Rp = 100
    while True:
        req = models.ListGroupsRequest()
        params = '''{
            "Page": ''' + str(Page) + ''',
            "Rp": ''' + str(Rp) + '''
        }'''
        req.from_json_string(params)
        resp = client.ListGroups(req)
        groups = resp.GroupInfo
        for group in groups:
            cam_group = CamGroup(group.GroupName, group.GroupId,
                                 group.Remark, group.CreateTime, client)
            cam_group.set_users()
            cam_group.set_policy()
            cam_groups.append(cam_group)

        if Page * Rp >= resp.TotalNum: break
        Page = Page + 1

def list_roles(client):
    global cam_roles
    if len(cam_roles) > 0: return

    Page = 1
    Rp = 100
    while True:
        req = models.DescribeRoleListRequest()
        params = '''{
            "Page": ''' + str(Page) + ''',
            "Rp": ''' + str(Rp) + '''
        }'''
        req.from_json_string(params)
        resp = client.DescribeRoleList(req)
        roles = resp.List
        for role in roles:
            cam_role = CamRole(role.RoleName, role.RoleId, role.RoleType,
                               role.Description, role.PolicyDocument, client)
            cam_role.set_policy()
            cam_roles.append(cam_role)

        if Page * Rp >= resp.TotalNum: break
        Page = Page + 1


def get_user_cos_bucket_auths(suin):
    cos_auths = []
    for bucket in cos_buckets:
        auth = {'Bucket': bucket.get_name(), 'acl':[], 'policy':[]}
        match = False
        acls = bucket.get_acl()
        for acl in acls:
            if str(acl).find(suin) > 0:
                match = True
                auth['acl'].append(acl)
        policy = bucket.get_policy()
        if policy:
            for state in policy['Statement']:
                if str(state).find(suin) > 0:
                    match = True
                    auth['policy'].append(state)
        if match: cos_auths.append(auth)
    return cos_auths

def get_user_group_cos_auths(suin):
    cos_auths = []
    for group in cam_groups:
        auth = {}
        users = group.get_users()
        policy = group.get_policy()
        if str(users).find(suin) > 0:
            auth['Name'] = group.get_name()
            if policy:
                str_policy = str(policy).lower()
                if str_policy.find("cos") > 0:
                    auth['Policy'] = policy
                    cos_auths.append(auth)
    return cos_auths

def analyse_user_cos_auths(user):
    bucket_auths = get_user_cos_bucket_auths(str(user.uin))
    group_auths = get_user_group_cos_auths(str(user.uin))
    print("CamUser: ", json_dump_object(user.dict()))
    print("\nCOS Buckets: ", json_dump_object(bucket_auths))
    print("\nCOS Related CamGroup:", json_dump_object(group_auths))
    return

def init_cos_buckets():
    global cos_buckets
    if len(cos_buckets) > 0: return

    buckets = get_buckets()
    for bucket in buckets:
        cos_client = init_cos_client(bucket['Location'])
        cos_bucket = CosBucket(bucket['CreationDate'], bucket['Name'],
                               bucket['Location'], cos_client)
        cos_bucket.set_acl()
        cos_bucket.set_policy()
        cos_buckets.append(cos_bucket)
        #break
    return

def analyse_user_with_uin(uin, client):
    list_users(client)
    for user in cam_users:
        if uin == str(user.get_uin()):
            analyse_user_cos_auths(user)
            break
    return

def analyse_user_with_name(name, client):
    req = models.GetUserRequest()
    params = '''{
        "Name": "''' + name + '''"
    }'''
    req.from_json_string(params)
    try:
        resp = client.GetUser(req)
        cam_user = CamUser(resp.Name, resp.Uid, resp.Uin,
                           resp.ConsoleLogin, resp.Remark, client)
        cam_user.set_policy()
        analyse_user_cos_auths(cam_user)
    except TencentCloudSDKException as err:
        print(err)
    return

def analyse_all_users(client):
    list_users(client)
    print("\n\n========= CAM USERS =========")
    index = 1
    for user in cam_users:
        print("\n----- %d -----" % index)
        analyse_user_cos_auths(user)
        index = index + 1
    return

def analyse_cos_bucket(name, client):
    for bucket in cos_buckets:
        if name == bucket.get_name():
            print(bucket)
            break
    return

def analyse_cos_cam_roles(client):
    list_roles(client)
    print("\n\n========= COS Related CAM Roles =========")
    index = 1
    for role in cam_roles:
        role_json = json_dump_object(role.dict())
        str_r = str(role_json).lower()
        if str_r.find("cos") < 0:
            continue
        print("\n----- %d -----" % index)
        print("CamRole: ", role_json)
        index = index + 1
    return

def main(argv):
    # Init cam client
    cred = credential.Credential(secret_id, secret_key)
    client = cam_client.CamClient(cred, "ap-shanghai")

    # Init cos bucket and cam group
    print("Initialize user related cos buckets and cam group info, waiting...")
    init_cos_buckets()
    list_groups(client)

    while True:
        v = input("\n============\n" \
                          "Input: user-name/uin/bucket-name? " \
                          "or r(camrole)? or a(all)? or q(quit)? ")
        if v == "q": break

        if is_digit(v):
            analyse_user_with_uin(v, client)
        elif is_cos_bucket_style(v):
            analyse_cos_bucket(v, client)
        elif v == "r":
            analyse_cos_cam_roles(client)
        elif v== "a":
            analyse_all_users(client)
            analyse_cos_cam_roles(client)
        else:
            analyse_user_with_name(v, client)
 
    return

## Main Entrance
if __name__ == "__main__":
    main(sys.argv)

