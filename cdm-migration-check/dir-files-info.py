#!/usr/bin/env python
# coding=utf-8

import sys
import string
import re
import os
import time

result_file = "dir-files.info"
result_fd = 0

dirs_info = []
files_info = []

def write_log(msg):
    global result_fd
    result_fd.write(msg+'\n')

def walk_dir(dir):
    global files_info, dirs_info
    dir_total_files = 0
    dir_total_size = 0
    for root, ds, fs in os.walk(dir):
        #print(root, ds, fs)
        for file in fs:
            dir_total_files += 1
            fpath = os.path.join(root, file)
            fsize = os.path.getsize(fpath)
            dir_total_size += fsize
            files_info.append(','.join([fpath, str(fsize)]))
    dirs_info.append(','.join([dir, str(dir_total_files), str(dir_total_size)]))
    #print("dir: %s, files: %d, size: %d" % (dir, dir_total_files, dir_total_size))

def analyses_dir(dir):
    global files_info, dirs_info
    for file in os.listdir(dir):
        if file.startswith('.'): continue
        fpath = os.path.join(dir, file)
        if os.path.isfile(fpath):
            fsize = os.path.getsize(fpath)
            files_info.append(','.join([fpath, str(fsize)]))
        elif os.path.isdir(fpath):
            walk_dir(fpath)

    write_log("All directory info:")
    for dinfo in dirs_info:
        write_log(dinfo)

    write_log("\nAll files info:")
    for finfo in files_info:
        write_log(finfo)
    return

def open_file_fd():
    global result_fd, result_file
    fpath = os.path.join("./", result_file)
    print("Write log to file:", fpath)
    result_fd = open(fpath, 'w')
    return

def close_file_fd():
    global result_fd
    if result_fd:
        result_fd.close()
    return

def main(argv):
    if len(sys.argv) > 2:
        print("Command:", argv[0])
        print("        ", argv[0], "<directory>")
        return

    dir = "./"
    if len(sys.argv) == 2:
        dir = argv[1]

    open_file_fd()
    analyses_dir(dir)
    close_file_fd()

#########
if __name__ == "__main__":
    main(sys.argv)

