#!/usr/bin/env python

from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client
from qcloud_cos import CosServiceError
from qcloud_cos import CosClientError

import requests
import json
import sys
import re
import os
import struct
import random
import time
import threading
import threadpool
from ftplib import FTP
from socket import *
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, FIRST_COMPLETED

FTP_MIGRATION = None

DOWNLOAD_THREADS_NUM = 8
UPLOAD_THREADS_NUM = 2

MASTER_IP = 'xxx.xx.xxx.xxx'
MASTER_PORT = 7890
MIG_DIR = "/root/migration/"
TMP_RECORD_FILE = MIG_DIR + "agent.inflight-migraion-lines"
COS_PREFIX = "cosprefix/"

# Initialize cos client
secret_id = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
secret_key = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
bucket = 'xxxxxx'
region = 'ap-xxxxxxxx'
token = None
COS_CLIENT = COSClient(secret_id, secret_key, region, token, bucket)

class LogFD:
    def __init__(self, bname):
        self._skipmigration_fd = open(MIG_DIR + "agent.skip-migration-files." + bname, 'a')
        self._error_fd = open(MIG_DIR + "agent.error-log." + bname, 'a')
        self._info_fd = open(MIG_DIR + "agent.info-log." + bname, 'a')
        self._donemigration_fd = open(MIG_DIR + "agent.done-migration-files." + bname, 'a')
        self._tlock = threading.Lock()

    def close(self):
        if self._skipmigration_fd: self._skipmigration_fd.close()
        if self._donemigration_fd: self._donemigration_fd.close()
        if self._error_fd: self._error_fd.close()
        if self._info_fd: self._info_fd.close()

    def flush(self):
        if self._skipmigration_fd: self._skipmigration_fd.flush()
        if self._donemigration_fd: self._donemigration_fd.flush()
        if self._error_fd: self._error_fd.flush()
        if self._info_fd: self._info_fd.flush()

    def skipmig_log(self, msg):
        self._tlock.acquire()
        self._skipmigration_fd.write(msg+'\n')
        self._tlock.release()

    def error_log(self, msg):
        self._tlock.acquire()
        self._error_fd.write(msg+'\n')
        self._tlock.release()

    def info_log(self, msg):
        self._tlock.acquire()
        self._info_fd.write(msg+'\n')
        self._tlock.release()

    def donemig_log(self, msg):
        self._tlock.acquire()
        self._donemigration_fd.write(msg+'\n')
        self._tlock.release()

class COSClient:
    def __init__(self, secret_id, secret_key, region, token, bucket):
        cos_config = CosConfig(Region=region, SecretId=secret_id, SecretKey=secret_key, Token=token)
        self._bucket = bucket
        self._client = CosS3Client(cos_config)

    def does_object_exist(self, key):
        try:
            object_info = self._client.head_object(self._bucket, key)
        except Exception as e:
            #print(str(e))
            return None
        return object_info

    def do_upload_object(self, fname, path):
        try:
            response = self._client.upload_file(
                    Bucket=self._bucket,
                    Key=path,
                    LocalFilePath=fname,
                    PartSize=100,
                    MAXThread=10,
                )
        except Exception as e:
            print(str(e))
            return False
        return True

class FTPMigrationAgent:
    def __init__(self, cosclient, logfd, check_cos_object):
        self._cos_client = cosclient
        self._log_fd = logfd

        # Record ftp files need upload to COS
        self._download_files_list = []
        self._download_tlock = threading.Lock()
        self._download_sem = threading.Semaphore(1)

        # Record ftp files need upload to COS
        self._upload_files_list = []
        self._upload_tlock = threading.Lock()
        self._upload_need_exit = False

        # Record ftp clients of each ftp server
        self._ftp_clients_list = []
        self._ftp_tlock = threading.Lock()

        # Record ftp clients of each ftp server
        self._mig_counter = 0
        self._mig_result = []
        self._mig_result_tlock = threading.Lock()

    def set_check_cos_object(self, value):
        self._check_cos_object = value

    def get_check_cos_object(self):
        return self._check_cos_object

    # Quit ftp clients
    def quit_ftp_clients(self):
        for fclients in self._ftp_clients_list:
            self.write_info_log(' | '.join(["Quit ftp client", fclients[0], str(fclients[1])]))
            for ftp in fclients[1]:
                ftp.quit()

    def quit(self):
        self.quit_ftp_clients()
        self._log_fd.close()

    def write_skipmig_log(self, msg):
        self._log_fd.skipmig_log(msg)

    def write_error_log(self, msg):
        self._log_fd.error_log(msg)

    def write_info_log(self, msg):
        self._log_fd.info_log(msg)

    def write_donemig_log(self, msg):
        self._log_fd.donemig_log(msg)

    def flush_log(self):
        self._log_fd.flush()

    def does_cos_object_exist(self, key):
        return self._cos_client.does_object_exist(key)

    def do_cos_upload_object(self, fname, path):
        result = self._cos_client.do_upload_object(fname, path)
        if result == False:
            self.write_error_log(' | '.join(["Upload object error", path]))
        return result

    def set_migration_counter(self, value):
        self._mig_result_tlock.acquire()
        self._mig_counter = value
        self._mig_result_tlock.release()

    def update_migration_result(self, msg):
        self._mig_result_tlock.acquire()
        self._mig_result.append(msg)
        #print("updated mig result:", self._mig_result)
        self._mig_result_tlock.release()

    def get_and_clean_migration_result(self):
        self._mig_result_tlock.acquire()
        mresult = self._mig_result
        self._mig_result = []
        self._mig_counter = 0
        self._mig_result_tlock.release()
        return mresult

    def pull_ftp_from_list(self, server):
        ftp = None
        self._ftp_tlock.acquire()
        for fclients in self._ftp_clients_list:
            print(fclients[0], server[0])
            if fclients[0] == server[0]:
                if len(fclients[1]):
                    ftp = fclients[1].pop()
        self._ftp_tlock.release()
        return ftp

    def push_ftp_to_list(self, server, ftp):
        self._ftp_tlock.acquire()
        get_matched = False
        for fclients in self._ftp_clients_list:
            if fclients[0] == server[0]:
                get_matched = True
                fclients[1].append(ftp)

        if get_matched == False:
            ftps = [ftp]
            self._ftp_clients_list.append([server[0], ftps])
        self._ftp_tlock.release()

    # Create new ftp client
    def get_new_ftp_client(self, user, server):
        try:
            ftp = FTP()
            ftp.connect(server[0], int(server[1]), timeout=10)
            ftp.login(user[0], user[1])
        except Exception as e:
            self.write_error_log(' | '.join(["Ffp login error", str(server), str(e)]))
            return None

        self.write_info_log(' | '.join(["Succeed create new ftp client ", str(server), str([ftp])]))
        return ftp

    def pull_download_file_from_list(self):
        self._download_tlock.acquire()
        line = ""
        llen = len(self._download_files_list)
        if llen:
            line = self._download_files_list.pop()
        self._download_tlock.release()
        return llen, line

    def push_download_file_to_list(self, line):
        self._download_tlock.acquire()
        self._download_files_list.append(line)
        self._download_tlock.release()

    def download_file_list_length(self):
        return len(self._download_files_list)

    def download_sem_acquire(self):
        self._download_sem.acquire()

    def download_sem_release(self):
        self._download_sem.release()

    def pull_upload_file_from_list(self):
        self._upload_tlock.acquire()
        finfo = []
        if len(self._upload_files_list):
            finfo = self._upload_files_list.pop()
        self._upload_tlock.release()
        return finfo

    def push_upload_file_to_list(self, fname, fpath, line):
        self._upload_tlock.acquire()
        self._upload_files_list.append([fname, fpath, line])
        self._upload_tlock.release()

    def wait_upload_done(self):
        while True:
            self._mig_result_tlock.acquire()
            if len(self._mig_result) == self._mig_counter:
                self._mig_result_tlock.release()
                break
            self._mig_result_tlock.release()
            time.sleep(0.1)

    def set_upload_need_exit(self):
        self._upload_tlock.acquire()
        self._upload_need_exit = True
        self._upload_tlock.release()

    def upload_need_exit(self):
        return self._upload_need_exit

#ftp://imsftp:imsftp@xx.xx.xx.xx:21/storage17/2016/xxxx.ts
def decode_line(line):
    tlist = re.split('/|@|:', line)
    user = tlist[3:5]
    server = tlist[5:7]
    fpath = '/'.join(tlist[7:])
    return user, server, fpath

def get_file_with_ncftpget(line, fbname):
    # Use trickle to limit each ftpget speed
    #trickle_prefix = "'trickle -s -d 1280"
    #ftp_get_cmd = ' '.join([trickle_prefix, "ncftpget -V -C", line, fbname])
    ftp_get_cmd = ' '.join(["ncftpget -V -C", line, fbname])
    output = os.system(ftp_get_cmd)
    if output:
        FTP_MIGRATION.write_error_log(' | '.join(["Get ftp file error", line, str(output)]))
        return False
    else:
        return True

def get_file_with_ftpclient(user, server, fpath, fbname):
    # Get ftp client
    ftp = FTP_MIGRATION.pull_ftp_from_list(server)
    if ftp == None:
        ftp = FTP_MIGRATION.get_new_ftp_client(user, server)

    # Check ftp client
    if ftp == None:
        FTP_MIGRATION.write_error_log(' | '.join(["Get ftp client error", line]))
        return False

    dname = os.path.dirname(fpath)
    bname = os.path.basename(fpath)
    dname = '~/' + dname

    # Download file from FTP Server
    try:
        ftp.cwd(dname)
        filename = 'RETR ' + bname
        # Open local file and write data
        f = open(fbname, 'wb')
        #ftp.retrbinary(filename, f.write, 1024)
        ftp.retrbinary(filename, f.write)
        f.close()

        # Change ftp work path back to users home dir
        #ftp.cwd('~')
    except Exception as e:
        FTP_MIGRATION.push_ftp_to_list(server, ftp)
        FTP_MIGRATION.write_error_log(' | '.join(["Get ftp file error", line, str(e)]))
        return False

    # push ftp client to global list
    FTP_MIGRATION.push_ftp_to_list(server, ftp)

    return True

def handle_line_ftpfile_download(line):
    line = line.strip('\n')
    line = line.strip()
    user, server, fpath = decode_line(line)

    # Check object exist or not
    if FTP_MIGRATION.get_check_cos_object():
        oinfo = FTP_MIGRATION.does_cos_object_exist(COS_PREFIX + fpath)
        if oinfo != None:
            FTP_MIGRATION.write_skipmig_log(line)
            FTP_MIGRATION.update_migration_result("done:" + line)
            return

    dname = os.path.dirname(fpath)
    bname = os.path.basename(fpath)
    dname = '~/' + dname

    rnumstr = str(random.randint(1,50))
    fbname = rnumstr + bname

    smtime = int(time.time() * 1000)

    # Download file from FTP Server
    result = get_file_with_ncftpget(line, fbname)
    #result = get_file_with_ftpclient(user, server, fpath, fbname)
    if result == False:
        try:
            if os.path.exists(fbname):
                os.remove(fbname)
        except Exception as e:
            FTP_MIGRATION.write_error_log(' | '.join(["Remove file error", fbname, str(e)]))
        FTP_MIGRATION.update_migration_result("error:" + line)
        return

    # Collect ftp download cost time
    emtime = int(time.time() * 1000)
    cmtime = emtime - smtime
    fsize = os.path.getsize(fbname)
    speedkb = round(fsize*1000/1024/cmtime, 2)
    FTP_MIGRATION.write_info_log(' | '.join(["Download", line, str(fsize), str(cmtime), str(speedkb) + 'KB/s']))

    # Push ftp file info to global list, upload threads will upload it to COS
    FTP_MIGRATION.push_upload_file_to_list(fbname, fpath, line)

def handle_lines_with_multithread(lines, tnum):
    handle_tpool = threadpool.ThreadPool(tnum)
    requests = threadpool.makeRequests(handle_line_ftpfile_download, lines)
    [handle_tpool.putRequest(req) for req in requests]
    handle_tpool.wait()

# Upload ftp file to COS
def upload_file_to_cos(ftpmigration, finfo):
    fname = finfo[0]
    fpath = COS_PREFIX + finfo[1]
    line = finfo[2]

    smtime = int(time.time() * 1000)

    # Upload file to COS
    result = ftpmigration.do_cos_upload_object(fname, fpath)
    if result == True:
        ftpmigration.write_donemig_log(line)
        ftpmigration.update_migration_result("done:" + line)
    else:
        ftpmigration.write_error_log(' | '.join(["Migration error", line]))
        ftpmigration.update_migration_result("error:" + line)

    # Collect ftp download cost time
    emtime = int(time.time() * 1000)
    cmtime = emtime - smtime
    fsize = os.path.getsize(fname)
    speedkb = round(fsize*1000/1024/cmtime, 2)
    FTP_MIGRATION.write_info_log(' | '.join(["Upload", line, str(fsize), str(cmtime), str(speedkb) + 'KB/s']))

    # Delete local file
    try:
        if os.path.exists(fname):
            os.remove(fname)
    except Exception as e:
        ftpmigration.write_error_log(' | '.join(["Remove file error", fname, str(e)]))

# Thread execute function used to upload ftp file to COS
def thread_upload_files_to_cos(ftpmigration):
    while True:
        finfo = ftpmigration.pull_upload_file_from_list()
        if len(finfo) == 0:
            if ftpmigration.upload_need_exit():
                if ftpmigration.download_file_list_length() == 0:
                    break
            ftpmigration.flush_log()
            time.sleep(1)
            continue

        upload_file_to_cos(ftpmigration, finfo)

def need_exit_loop():
    fpath = 'EXIT'
    exist = os.path.exists(fpath)
    if exist == False:
        return False

    fd = open(fpath, 'r')
    lines = fd.readlines()
    fd.close()
    if len(lines) != 1:
        return False
    line = lines[0].strip('\n')
    if line == 'true':
        return True
    return False

def start_upload_file_threads(ftpmigration, num):
    executor = ThreadPoolExecutor(max_workers=num)
    all_tasks = [executor.submit(thread_upload_files_to_cos, (ftpmigration)) for count in range(1, num+1)]
    return all_tasks

def thread_download_files_to_cos(ftpmigration):
    while True:
        llen, line = ftpmigration.pull_download_file_from_list()
        if llen == 0:
            if ftpmigration.upload_need_exit():
                break
            ftpmigration.flush_log()
            time.sleep(1)
            continue

        # if length of download file list too small, wake up thread to get more from master
        if llen < DOWNLOAD_THREADS_NUM:
            ftpmigration.download_sem_release()

        handle_line_ftpfile_download(line)

def start_download_file_threads(ftpmigration, num):
    executor = ThreadPoolExecutor(max_workers=num)
    all_tasks = [executor.submit(thread_download_files_to_cos, (ftpmigration)) for count in range(1, num+1)]
    return all_tasks

def recv_master_data(client_socket):
    header_struct = client_socket.recv(4)
    unpack_res = struct.unpack('i',header_struct)
    data_size = unpack_res[0]
    rev_size  = 0
    master_data = b""
    while rev_size < data_size:
        recv_data = client_socket.recv(1024)
        #print("recv data", len(recv_data))
        rev_size += len(recv_data)
        master_data += recv_data
    #print("master data", len(master_data))
    return master_data

def get_migration_lines_from_master(ftpmigration):
    tcp_client_socket = socket(AF_INET, SOCK_STREAM)
    tcp_client_socket.connect((MASTER_IP, MASTER_PORT))

    mig_result = ftpmigration.get_and_clean_migration_result()
    if len(mig_result) == 0:
        mig_result.append("NULLRESULT")
    data_tx = json.dumps(mig_result)

    # step1: send data size, step2: send all data
    header = struct.pack('i', len(data_tx))
    tcp_client_socket.send(header)
    tcp_client_socket.send(data_tx.encode("utf-8"))

    lines = []
    master_data = recv_master_data(tcp_client_socket)
    if len(master_data) > 10:
        rlist = json.loads(master_data)
        check_object = rlist[0]
        if check_object == "NOCHECK":
            ftpmigration.set_check_cos_object(False)
        elif check_object == "CHECK":
            ftpmigration.set_check_cos_object(True)
        lines = rlist[1:]
    else:
        print("Maybe Done Mark from master:", master_data)

    # Write receive data to temp record file
    fd = open(TMP_RECORD_FILE, 'w')
    fd.writelines(lines)
    fd.close()

    # Cleanup
    tcp_client_socket.close()
    return lines

def do_one_loop_migraiton(ftpmigration, lines):
    # Set the files counter in this loop
    ftpmigration.set_migration_counter(len(lines))

    # Do migration with multithreads
    handle_lines_with_multithread(lines, DOWNLOAD_THREADS_NUM)

    # Wait all files had uploaded to cos
    ftpmigration.wait_upload_done()
    ftpmigration.flush_log()

def redo_migration_mabe_inflight(ftpmigration):
    if os.path.exists(TMP_RECORD_FILE) == False:
        return

    fd = open(TMP_RECORD_FILE, 'r')
    lines = fd.readlines()
    fd.close()
    if len(lines) == 0:
        return

    print("redo migration data: ", lines)
    oldvalue = ftpmigration.get_check_cos_object()
    ftpmigration.set_check_cos_object(True)
    do_one_loop_migraiton(ftpmigration, lines)
    ftpmigration.set_check_cos_object(oldvalue)

# Do file migration from ftp to COS
def do_file_migration(ftpmigration):
    # Start threads used to download file from FTP
    download_tasks = start_download_file_threads(ftpmigration, DOWNLOAD_THREADS_NUM)

    # Start threads used to upload file to COS
    upload_tasks = start_upload_file_threads(ftpmigration, UPLOAD_THREADS_NUM)

    # Check temp record file and do migration which maybe NOT Completed
    redo_migration_mabe_inflight(ftpmigration)

    # Do file migration dispatched by master
    num_lines = 0
    while True:
        ftpmigration.download_sem_acquire()
        lines = get_migration_lines_from_master(ftpmigration)
        if len(lines) == 0: break

        num_lines = num_lines + len(lines)
        for line in lines:
            ftpmigration.push_download_file_to_list(line)

        ftpmigration.write_info_log("Get %d lines from master" % num_lines)

        if need_exit_loop():
            print("NEED EXIT NOW!")
            break

    # Wait for upload tasks completed
    ftpmigration.set_upload_need_exit()
    wait(download_tasks, return_when=ALL_COMPLETED)
    wait(upload_tasks, return_when=ALL_COMPLETED)

# Default, DO NOT check cos object exist.
def main(argv):
    if len(sys.argv) != 2:
        print("Command:", argv[0], "true/false")
        return
    check_cos_object = False
    if argv[1] == 'true':
        check_cos_object = True

    log_fd = LogFD("agent")

    global FTP_MIGRATION
    ftpmigration = FTPMigrationAgent(COS_CLIENT, log_fd, check_cos_object)
    FTP_MIGRATION = ftpmigration
    do_file_migration(ftpmigration)
    ftpmigration.quit()

#########
debug = False
#debug = True
if __name__ == "__main__":
    main(sys.argv)

