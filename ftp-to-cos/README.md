## 1. 需求背景
支持根据客户给定的list从FTP下载文件并迁移到COS的功能，支持迁移进度监控等。

### 实现功能
`Master-Agents FTP-TO-COS`工具是COS侧开发的针对FTP文件列表的迁移工具，支持如下功能:
- 基于FTP文件的列表迁移文件到COS上
- 支持多线程并发来提高迁移效率
- 支持Master-Agents模式，可以线性扩展
- 支持幂等执行，迁移过的文件自动跳过
- 记录迁移成功和失败的日志，失败的可以单独触发重试
- 输出迁移相关的metrics，支持迁移进度监控等

### 相关脚本
实现该功能的脚本如下：
```sh
# tree
.
└── ftp-to-cos
    ├── ftp-to-cos-agent.py
    └── ftp-to-cos-master.py
```

### 脚本执行
配置Master python脚本：ftp-to-cos-master.py
```sh
-- 配置Agents的IP
AGENTS_LIST = ["xxx.xx.xxx.152",\
               "xxx.xx.xxx.153"]

-- 配置迁移目录
MIG_DIR = "/root/migration/"
```

配置Agent python脚本：ftp-to-cos-agent.py
```sh
-- 配置并行下载和上传的线程数
DOWNLOAD_THREADS_NUM = 8
UPLOAD_THREADS_NUM = 2

-- 配置master节点的IP和端口
MASTER_IP = 'xxx.xx.xxx.152'
MASTER_PORT = 7890

-- 配置迁移目录
MIG_DIR = "/root/migration/"
TMP_RECORD_FILE = MIG_DIR + "agent.inflight-migraion-lines"

-- 配置COS添加的前缀
COS_PREFIX = "cosprefix/"
```

配置上述两个脚本后，在Master节点执行脚本：
```sh
# python3 ftp-to-cos-master.py <ftp-migration-list>
```
