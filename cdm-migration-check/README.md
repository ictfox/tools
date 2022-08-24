## CDM迁移数据后的校验需求

### 解决步骤
1. 在客户搬迁数据的源目录，运行脚本获取源文件的信息
2. 待文件通过CDM迁移上云后，运行脚本对比源文件和COS Bucket上的对象

### 相关脚本
实现该功能的脚本如下：

```sh
# tree
.
├── cdm-mig-check.py
└── dir-files-info.py
```

- dir-files-info.py: 获取本地目录或指定目录下所有文件的信息；

- cdm-mig-check.py: 对比原始目录文件和COS Bucket上的对象，输出不一致的信息；

### 执行脚本
1. **dir-files-info.py**
在迁移的源目录下运行脚本，比如指定目录为`2022`：

```sh
# python3 dir-files-info.py 2022
Write log to file: ./dir-files.info
```

输出后的文件内容，示例如下：

```sh
# cat ./dir-files.info
All directory info:
2022/0808,2130,2083449643975
2022/0809,2852,71273689273819
...

All files info:
2022/0808/10点监控/下午/截屏2022-08-08 下午07.52.jpg,614253
2022/0808/11点监控/下午/._.DS_Store,4096
...
2022/0809/录屏/2022-08-09 21-08-30.mp4,2557885738
2022/0809/录屏/截屏2022-08-09 上午8.21.png,3278728
...
```


2. **cdm-mig-check.py**
在CDM设备的数据都迁移上云后，配置访问COS Bucket需要的环境变量，然后检查源文件和COS Bucket上的对象是否一致。

- 配置子账号的ak/sk（有指定Bucket的`listobject/headobject`权限）, COS Bucket和Region信息

```sh
# vim cdm-mig-check.py
...
secret_id = os.getenv('SECRET_ID')
secret_key = os.getenv('SECRET_KEY')
cos_bucket = os.getenv('COS_BUCKET')
cos_region = os.getenv('COS_REGION')
...

# export SECRET_ID="xxxxxxxxxxxx"
# export SECRET_KEY="xxxxxxxxxxx"
# export COS_BUCKET="xxxxxxxxxxxx"
# export COS_REGION="xxxxxxxxxxx"
```

- 然后运行脚本，指定目录或者前缀信息

```sh
# python3 cdm-mig-check.py -h
usage: cdm-mig-check.py [-h] [-d DIRECTORY] [-p PREFIX] file

Check CDM migration result with specified original files info.

positional arguments:
  file                  file with original files info

optional arguments:
  -h, --help            show this help message and exit
  -d DIRECTORY, --directory DIRECTORY
                        just check specified directory
  -p PREFIX, --prefix PREFIX
                        check with cos prefix
```
> 参数file：为dir-files-info.py运行的输出文件
> 参数directory：指定校验的目录
> 参数prefix：指定COS Bucket上对象的前缀

运行脚本后的输出示例如下：

```sh
# python3 cdm-mig-check.py dir-files.info -p 2022 -d 0809
Write result to file:  cdm-mig-check.result
Check files in directory:  0809

# cat cdm-mig-check.result
Diff with prefix: ./0809
 - Orig files: 2557, size: 23223680773813
 - Migd files: 2548, size: 23223680734901
./0809/机器/._ATOM,4096
./0809/11点监控/.DS_Store,14340
./0809/18点监控/.DS_Store,4096
...
```
