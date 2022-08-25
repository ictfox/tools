#!/usr/bin/env python
# coding=utf-8

import sys
import string
import re
import os
import time
import logging
import argparse

from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client
from qcloud_cos import CosServiceError
from qcloud_cos import CosClientError

secret_id = os.getenv('SECRET_ID')
secret_key = os.getenv('SECRET_KEY')
cos_bucket = os.getenv('COS_BUCKET')
cos_region = os.getenv('COS_REGION')

# cos_prefix could be '' or 'prefix' or 'perfix/'
cos_prefix = ''

result_file = "cdm-mig-check.result"
result_fd = 0

#logging.basicConfig(level=logging.INFO, stream=sys.stdout)

config = CosConfig(Region=cos_region, SecretId=secret_id, SecretKey=secret_key, Token=None)
client = CosS3Client(config)

def write_log(msg):
    global result_fd
    result_fd.write(msg+'\n')

def object_info(key):
    try:
        oinfo = client.head_object(cos_bucket, key)
    except Exception as e:
        return None
    return oinfo

def get_orig_files_with_prefix(orig_file, prefix):
    ofinfo = set()
    with open(orig_file) as file:
        for line in file:
            line = line.strip('\n')
            if line.startswith(prefix + '/'):
                ofinfo.add(line)
                continue
            else:
                if len(ofinfo):
                    break
                continue
    return ofinfo

def get_orig_files_in_top_dir(orig_file):
    ofinfo = set()
    started = False
    base_dir = ""
    with open(orig_file) as file:
        for line in file:
            if started == False:
                if line.startswith('All files info:'):
                    started = True
                else:
                    if base_dir == "":
                        base_dir = os.path.dirname(line)
                continue

            line = line.strip('\n')
            if os.path.dirname(line) == base_dir:
                ofinfo.add(line)
                continue
    return ofinfo

def get_migd_files_with_prefix(prefix):
    mfinfo = set()
    mfsize = 0

    list_prefix  = os.path.relpath(os.path.join(cos_prefix, prefix))
    marker = ""
    while True:
        response = client.list_objects(
            Bucket=cos_bucket,
            Prefix=list_prefix + '/',
            Marker=marker,
        )
        if 'Contents' in response:
            lists = response['Contents']
            for i in lists:
                okey = i.get("Key")
                osize = i.get("Size")
                mfsize += int(osize)
                #print(okey, osize)
                mfinfo.add(okey.replace(list_prefix, prefix, 1) + "," + osize)

        if response['IsTruncated'] == 'false':
            break
        marker = response['NextMarker']
    return mfinfo, mfsize

def check_specified_files(files):
    minfo = set()
    for file in files:
        flist = file.split(',')
        fkey = ','.join(flist[:-1])
        okey = os.path.relpath(os.path.join(cos_prefix, fkey))
        oinfo = object_info(okey)
        if oinfo == None:
            continue
        minfo.add(fkey + "," + oinfo['Content-Length'])
    return minfo

def check_files_in_top_dir(orig_file):
    files_orig = get_orig_files_in_top_dir(orig_file)
    files_migd = check_specified_files(files_orig)
    write_log("Diff in top directory: ")
    write_log(" - Orig files: " + str(len(files_orig)))
    write_log(" - Migd files: " + str(len(files_migd)))
    sdiff = files_orig.symmetric_difference(files_migd)
    for item in sdiff:
        write_log(item)
    write_log("")

def check_migration_result(orig_file, specified_dir):
    # Check files in top directory
    if specified_dir == "":
        check_files_in_top_dir(orig_file)

    # Check files with directory prefix
    dirs_info = []
    with open(orig_file) as file:
        for line in file:
            line = line.strip('\n')
            dir_info = line.split(',')
            if len(dir_info) == 3:
                dirs_info.append(dir_info)
                continue
            if line.startswith('All directory info:'):
                continue
            elif line.startswith('All files info:'):
                break

    for dir in dirs_info:
        prefix = dir[0]
        files_count = int(dir[1])
        files_size = int(dir[2])
        #print(prefix, files_count, files_size)

        if specified_dir:
            if os.path.relpath(specified_dir) == os.path.relpath(prefix):
                print("Check files in directory: ", specified_dir)
            else:
                continue

        files_orig = get_orig_files_with_prefix(orig_file, prefix)
        files_migd, size_migd = get_migd_files_with_prefix(prefix)
        write_log("Diff with prefix: " + prefix)
        write_log(" - Orig files: " + str(files_count) + ", size: " + str(files_size))
        write_log(" - Migd files: " + str(len(files_migd)) + ", size: " + str(size_migd))
        sdiff = files_orig.symmetric_difference(files_migd)
        for item in sdiff:
            write_log(item)
        write_log("")

    return

def open_file_fd():
    global result_fd, result_file
    print("Write result to file: ", result_file)
    result_fd = open(result_file, 'w')
    return

def close_file_fd():
    global result_fd
    if result_fd: result_fd.close()
    return

def main(argv):
    global cos_prefix
    parser = argparse.ArgumentParser(description='Check CDM migration result with specified original files info.')
    parser.add_argument("-d", "--directory", default='', type=str, help="just check specified directory")
    parser.add_argument("-p", "--prefix", default='', type=str, help="check with cos prefix")
    parser.add_argument("file", type=str, help="file with original files info")
    args = parser.parse_args()

    orig_file = args.file
    specified_dir = args.directory
    cos_prefix = args.prefix

    open_file_fd()
    check_migration_result(orig_file, specified_dir)
    close_file_fd()

#########
if __name__ == "__main__":
    main(sys.argv)

