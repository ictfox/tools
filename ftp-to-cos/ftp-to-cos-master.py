import os
import sys
import struct
import random
import time
import json
import threading
import threadpool
from socket import *
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, FIRST_COMPLETED
from random import randint
from flask import Flask, Response
from prometheus_client import Counter, Gauge, generate_latest, CollectorRegistry

LISTEN_PORT = 7890

AGENTS_LIST = ["xxx.xx.xxx.12",\
               "xxx.xx.xxx.13",\
               "xxx.xx.xxx.14",\
               "xxx.xx.xxx.15",\
               "xxx.xx.xxx.16"]

MIG_DIR = "/root/migration/"

AGENT_CHECK_CMD = "ps aux | grep ftp-to-cos-agent.py | grep -v 'grep' | grep -v 'ssh'"
AGENT_START_CMD = "python3 " + MIG_DIR + "ftp-to-cos-agent.py true"
AGENT_NETSTAT_CMD = "ifconfig eth0 | grep bytes | awk '{print $5}' && date +%s%3N"

# Use Flask to report migration result to prometheus
mig_app = Flask(__name__)
registry = CollectorRegistry()
success_counter = Counter('migration_success_files', 'files migration succeed to cos', registry=registry)
failed_counter = Counter('migration_failed_files', 'files migration failed to cos', registry=registry)
total_counter = Counter('migration_total_files', 'files migration failed to cos', registry=registry)
netstat_input = Gauge('netstat_input', 'netstat input value in Kbps', ['machine_ip'], registry=registry)
netstat_output = Gauge('netstat_output', 'netstat output value in Kbps', ['machine_ip'], registry=registry)

@mig_app.route('/metrics')
def mig_result_metrics():
    return Response(generate_latest(registry), mimetype='text/plain')

class LogFD:
    def __init__(self, bname):
        self._skipmigration_fd = open(MIG_DIR + "Master.skip-migration-files." + bname, 'a')
        self._error_fd = open(MIG_DIR + "Master.error-log." + bname, 'a')
        self._info_fd = open(MIG_DIR + "Master.info-log." + bname, 'a')
        self._donemigration_fd = open(MIG_DIR + "Master.done-migration-files." + bname, 'a')
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

class FTPMigrationMaster:
    def __init__(self, agents, logfd):
        self._agents = agents
        self._log_fd = logfd
        self._done_mig = False
        self._dispatched_lines = 0

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

    def done_migration(self):
        return self._done_mig

    def set_done_migration(self, value):
        self._done_mig = value

    def do_remote_cmd(self, host, cmd):
        #self.write_info_log(' '.join(["null return: ssh", host, cmd]))
        out = os.system("ssh " + host + " " + cmd)
        if out:
            self.write_error_log(' '.join(["Error code", out]))
        return

    def do_remote_cmd_with_return(self, host, cmd):
        #self.write_info_log(' '.join(["with return: ssh", host, cmd]))
        output_list = os.popen("ssh " + host + " " + cmd).readlines()
        return output_list

    def get_agents_netstat(self):
        nstat = []
        for agent in self._agents:
            ol = self.do_remote_cmd_with_return(agent, AGENT_NETSTAT_CMD)
            #print("agent netstat:", agent, ol)
            nstat.append([agent] + ol)
        return nstat
 
    def check_start_agents(self):
        for agent in self._agents:
            output_list = self.do_remote_cmd_with_return(agent, AGENT_CHECK_CMD)
            if len(output_list) == 0:
                cmd = AGENT_START_CMD + ">>" + agent + ".log &"
                self.do_remote_cmd(agent, cmd)

    # Wait all agents quit
    def wait_agents_quit(self):
        while True:
            quit_agents = 0
            for agent in self._agents:
                output_list = self.do_remote_cmd_with_return(agent, AGENT_CHECK_CMD)
                if len(output_list) == 0:
                    quit_agents += 1

            if quit_agents == len(self._agents):
                break

    def inc_dispatched_lines(self, len):
        self._dispatched_lines += len

    def update_migration_result(self, results):
        global success_counter, failed_counter
        for result in results:
            if result.startswith('done:'):
                self.write_donemig_log(result[5:])
                success_counter.inc(1)
            elif result.startswith('error:'):
                self.write_error_log(result[6:])
                failed_counter.inc(1)
            else:
                print("Error migation result: %s" % result)

    def quit(self):
        self.wait_agents_quit()
        self._log_fd.close()

def recv_agent_data(client_socket):
    header_struct = client_socket.recv(4)
    unpack_res = struct.unpack('i',header_struct)
    data_size = unpack_res[0]
    rev_size  = 0
    agent_data = b""
    while rev_size < data_size:
        recv_data = client_socket.recv(1024)
        #print("recv data", len(recv_data))
        rev_size += len(recv_data)
        agent_data += recv_data
    #print("agent data", len(agent_data))
    return agent_data

def dispatch_and_wait_agents_done(ftpmigration, fpath, check_lines):
    rdata_fd = open(fpath, 'r', errors='replace')

    tcp_master_socket = socket(AF_INET, SOCK_STREAM)
    address = ('', LISTEN_PORT)
    tcp_master_socket.bind(address)
    tcp_master_socket.listen(128)

    num_lines = 0
    while True:
        client_socket, clientAddr = tcp_master_socket.accept()

        agent_data = recv_agent_data(client_socket)
        try:
            agent_msg = json.loads(agent_data)
            #print("recv agent msg:", agent_msg)
            ftpmigration.update_migration_result(agent_msg)
        except Exception as e:
            print("exception:", e)
            print("recv agent data:", agent_data)

        msg_header = ["NOCHECK"]
        if num_lines < check_lines:
            msg_header = ["CHECK"]

        mig_lines = rdata_fd.readlines(5000) # characters
        random.shuffle(mig_lines)
        if mig_lines:
            data_tx = json.dumps(msg_header + mig_lines)
        else:
            data_tx = "DONE"
            ftpmigration.set_done_migration(True)
        num_lines = num_lines + len(mig_lines)

        # step1: send data size, step2: send all data
        header = struct.pack('i', len(data_tx))
        client_socket.send(header)
        client_socket.sendall(data_tx.encode("utf-8"))
        client_socket.close()

        ftpmigration.inc_dispatched_lines(len(mig_lines))
        ftpmigration.flush_log()

        if ftpmigration.done_migration():
            print("Done Migration!")

    tcp_master_socket.close()
    rdata_fd.close()

def thread_agents_check_start(ftpmigration):
    while True:
        if ftpmigration.done_migration():
            break
        ftpmigration.check_start_agents()
        time.sleep(60)

def start_agents_check_threads(ftpmigration, num):
    executor = ThreadPoolExecutor(max_workers=num)
    all_tasks = [executor.submit(thread_agents_check_start, (ftpmigration)) for count in range(1, num+1)]
    return all_tasks

class flaskThread(threading.Thread):
    def __init__(self, name):
        threading.Thread.__init__(self)
        self.name = name

    def run(self):
        mig_app.run(host='0.0.0.0', port=5000)

class NetStatThread(threading.Thread):
    def __init__(self, name, ftpmigration):
        threading.Thread.__init__(self)
        self.name = name
        self.ftpmigration = ftpmigration

    def run(self):
        interval = 15
        ns_bytes_last = {}
        first_time = True
        while True:
            agents_ns = self.ftpmigration.get_agents_netstat()
            #print("netstat:", agents_ns)
            for ns in agents_ns:
                ipaddr = ns[0]
                rx_bytes = float(ns[1].strip('\n'))
                tx_bytes = float(ns[2].strip('\n'))
                run_time = float(ns[3].strip('\n'))
                if first_time:
                    ns_bytes_last[ipaddr] = [run_time, rx_bytes, tx_bytes]
                    continue

                interval_s = round((run_time - ns_bytes_last[ipaddr][0])/1000, 3)
                rx_speeds = round((rx_bytes - ns_bytes_last[ipaddr][1])/1024/interval_s, 2)
                tx_speeds = round((tx_bytes - ns_bytes_last[ipaddr][2])/1024/interval_s, 2)
                #self.ftpmigration.write_info_log(' '.join([ipaddr, str(interval_s), str(rx_speeds), str(tx_speeds)]))
                ns_bytes_last[ipaddr] = [run_time, rx_bytes, tx_bytes]
                netstat_input.labels(ipaddr).set(rx_speeds)
                netstat_output.labels(ipaddr).set(tx_speeds)

            first_time = False
            time.sleep(interval)

# python3 xxx.py
def main(argv):
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Command:", argv[0], "ftp-list")
        print("Command:", argv[0], "ftp-list check-lines")
        return
    fpath = argv[1]

    check_lines = 0
    if len(sys.argv) == 3:
        check_lines = int(argv[2])

    # Get total lines need do migration
    global total_counter
    cmd = "wc -l " + fpath
    output_list = os.popen(cmd).readlines()
    if output_list:
       	finfo = output_list[0].split()
        total_counter.inc(int(finfo[0]))

    # Start flask thread
    flask_thread = flaskThread("flask daemon")
    flask_thread.start()

    bname = os.path.basename(fpath)
    logfd = LogFD(bname)
    ftpmigration = FTPMigrationMaster(AGENTS_LIST, logfd)

    # Start netstat thread
    netstat_thread = NetStatThread("netstat daemon", ftpmigration)
    netstat_thread.start()

    # Start all agents task
    agents_task = start_agents_check_threads(ftpmigration, 1)

    # Dispatch file migration to agents
    dispatch_and_wait_agents_done(ftpmigration, fpath, check_lines)

    # Wait agents task completed
    wait(agents_task, return_when=ALL_COMPLETED)
    ftpmigration.quit()

    # Wait thread
    flask_thread.join()
    netstat_thread.join()

#########
if __name__ == "__main__":
    main(sys.argv)
