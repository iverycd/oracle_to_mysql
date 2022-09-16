# oracle_to_mysql

![commit](https://img.shields.io/github/last-commit/iverycd/oracle_to_mysql?style=flat-square)
![tag](https://img.shields.io/github/v/release/iverycd/oracle_to_mysql?display_name=tag)
![languages](https://img.shields.io/github/languages/top/iverycd/oracle_to_mysql)

## 简介

功能特性

- 支持运行在Linux以及Windows环境，已测试通过Oracle 11.2.0.4及以上，MySQL 5.7及以上

- 支持在线迁移Oracle到MySQL的表、视图、索引、触发器、外键、自增列、以及部分触发器，不支持存储过程以及函数的迁移

- 自动分析源库Oracle字段的长度并适配到MySQL合适的长度，避免MySQL字段长度太长问题 “Row size too large 65535”

- 自动处理Oracle触发器+序列形式的自增列适配到MySQL原生自增列

- 转储Oracle的函数、存储过程到平面文件以便修改人工翻译适配到MySQL

- 后台多进程数据迁移，数据迁移更快

- 支持迁移部分表结构以及数据功能

- 支持迁移Oracle与MySQL共同存在的列数据，即目标数据库的列字段是源数据库列字段的超集

- 记录迁移日志，转储表、视图等DDL对象创建失败的sql语句

- 支持Oracle与MySQL表数量快速比对功能

- 可放到后台一键迁移Oracle到MySQL


环境要求

- 在运行的客户端PC需要同时能连通源端Oracle数据库以及目标MySQL数据库

- 支持Win 10、Centos 7


运行概览

`全库迁移`

![image](https://user-images.githubusercontent.com/35289289/190549811-62bce516-c8e4-4ca1-a751-2c718ec6f460.png)


`比对数据`

![image](https://user-images.githubusercontent.com/35289289/190549333-8cc98ec1-f88a-417e-9628-0c1228f4b2bd.png)

`记录迁移失败的DDL`

![image](https://user-images.githubusercontent.com/35289289/190550154-200c3fe4-3a1c-4bec-9243-ddb16faa516c.png)

`转储索引、视图、存储过程、函数DDL`

![image](https://user-images.githubusercontent.com/35289289/190550380-d6ce3075-aa1f-4d20-9708-6311e3542170.png)

`多进程并行迁移表数据`

![image](https://user-images.githubusercontent.com/35289289/190551349-7a00d764-efc2-482d-b14e-03252446d1fa.png)

![image](https://user-images.githubusercontent.com/35289289/190551529-dee0b5e4-fdd9-4ea2-af80-8d68d2d021bf.png)



## Dev环境运行
1、oracle客户端环境准备

`Linux`

下载Linux Instant Client（11.2.0.4及以上都行)

https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html


![image](https://user-images.githubusercontent.com/35289289/190542155-1883896f-38af-4693-b72a-577f320cd62e.png)

或者直接下载当前资源库文件[linux_oracle_client.7z](https://github.com/iverycd/oracle_to_mysql/blob/master/linux_oracle_client.7z)

以上解压并设定环境变量
```bash
echo "export ORACLE_HOME=/opt/oracle_to_mysql/ora_client/instantclient_11_2"
echo "export LD_LIBRARY_PATH=$ORACLE_HOME:$LD_LIBRARY_PATH"
echo "export PATH=$ORACLE_HOME:$PATH"
```

`Win`

下载Windows Instant Client（11.2.0.4及以上都行)

https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html

![image](https://user-images.githubusercontent.com/35289289/190544173-9ba6264f-7df7-46cd-acc2-08b5605ac7bf.png)

或者直接下载当前资源库文件[win_oracle_client.7z](https://github.com/iverycd/oracle_to_mysql/blob/master/win_oracle_client.7z)

解压之后重命名目录名称为`oracle_client`并放到代码同路径，如下：

![image](https://user-images.githubusercontent.com/35289289/190544799-b1c5e880-4582-4395-b990-d326d5ae613f.png)


2、编辑config文件

以下分别填入源库和目标库的信息

```bash
config.ini 

[oracle]
host = 192.168.212.23
port = 1521
user = admin
passwd = admin123
service_name = orcl

[mysql]
host = 192.168.209.24
port = 3306
user = root
passwd = Gep
database = temptest
dbchar = utf8mb4
```

3、全库迁移

```python
python oracle_mig_mysql.py
```


## 如何打包

先修改脚本内oracle_client路径以及Python环境路径

`Linux`打包

在代码根目录创建hooks目录并编写配置文件

![image](https://user-images.githubusercontent.com/35289289/190604823-81b3f70e-94cb-48ef-89a5-f12271efa979.png)

![image](https://user-images.githubusercontent.com/35289289/190604919-9ff404e8-4df0-480f-8346-6e6f226b5412.png)

```bash
mkdir hooks

vi hooks/hook-prettytable.py
内容如下：
  
from PyInstaller.utils.hooks import collect_data_files, copy_metadata
datas = collect_data_files('prettytable') + copy_metadata('prettytable')

运行打包脚本 `sh pack.sh`
```
 



`Win`

运行bat脚本 `pack.bat`

## 开箱即用的二进制可执行文件

下载[release](https://github.com/iverycd/oracle_to_mysql/releases/tag/v1.9.15.2)

解压之后即可运行此工具

1、解压，例如将`oracle_mig_mysql.zip`上传到/opt目录

2、在root用户下解压

```bash
[root@localhost root]# cd /opt

[root@localhost root]# unzip oracle_mig_mysql.zip
```

3、运行环境变量脚本
```bash
[root@localhost ]# sh /opt/oracle_to_mysql/env_ora.sh && source /root/.bash_profile

注意：此步骤3命令只需执行一次即可
```


### 全库迁移

#### Linux环境

1、进入迁移工具目录

```bash
[root@localhost opt]# cd /opt/oracle_to_mysql
```

2、编辑配置文件 config.ini文件，修改源数据库以及目标数据库信息

```bash

[root@localhost ]# vi config.ini 

[oracle]
host = 192.168.21.2
port = 1521
user = admin
passwd = admin123
service_name = orcl

[mysql]
host = 192.168.20.2
port = 3306
user = root
passwd = Ge
database = temptest
dbchar = utf8mb4

```


3、执行全库迁移

`须知：如果是通过堡垒机或者是vpn连接的非图形化界面，强烈建议使用后台方式运行此工具，避免数据迁移中断`

```bash

后台执行命令：

[root@localhost ]# nohup ./oracle_mig_mysql -q &

前台执行命令：（不推荐，如果源库数据量很大，建议使用后台迁移，避免putty等终端工具超时自动断开）

[root@localhost ]#  ./oracle_mig_mysql

```


4、查看数据迁移运行过程

如果是在后台运行“nohup ./oracle_mig_mysql -q &”

，可通过如下命令查看实时迁移过程

```bash


[root@localhost ]# tail -100f nohup.out 


```


迁移已经完成

![image](https://user-images.githubusercontent.com/35289289/190546784-4cddf41f-b38b-41f0-a669-978e25afc64d.png)



5、迁移完成后查看迁移任务日志

[root@localhost ]# cd /opt/oracle_to_mysql/mig_log/

此文件夹下面以时间戳名命的子文件夹内有如下日志需要关注：

`ddl_failed_table.log` --> 创建失败的表、视图、索引、触发器等对象的DDL创建语句，此部分需要修改语法适配并重新在MySQL创建即可

`insert_failed_table.log ` --> 表数据插入失败的对象名称，此部分需要重新对个别表重新迁移数据

`ddl_function_procedure.sql`  --> Oracle导出的存储过程以及函数的定义，此部分需要修改语法适配并重新在MySQL创建即可


#### Windows环境

Windows环境与Linux环境类似，下载压缩包之后解压到任意目录

1、编辑配置文件 config.ini文件，修改源数据库以及目标数据库信息

```bash
[oracle]
host = 192.168.212.23
port = 1521
user = admin
passwd = admin123
service_name = orcl

[mysql]
host = 192.168.209.24
port = 3306
user = root
passwd = Gep
database = temptest
dbchar = utf8mb4
```

2、cmd进入迁移工具目录



```bash
在cmd命令行窗口内执行

进入工具所在目录

C:\Users\Administrator>g:

G:\>cd G:\oracle_mig_mysql
```


3、执行全库迁移


```bash
G:\oracle_mig_mysql>oracle_mig_mysql.exe
```

4、迁移完成后查看迁移任务日志


迁移工具目录下面会生成以时间戳名命的日志需要关注：


`ddl_failed_table.log` --> 创建失败的表、视图、索引、触发器等对象的DDL创建语句，此部分需要修改语法适配并重新在MySQL创建即可

`insert_failed_table.log ` --> 表数据插入失败的对象名称，此部分需要重新对个别表重新迁移数据

`ddl_function_procedure.sql`  --> Oracle导出的存储过程以及函数的定义，此部分需要修改语法适配并重新在MySQL创建即可


## 比对Oracle以及MySQL表数据量

oracle_compare_mysql.py，可以在数据迁移完成后快速比对Oracle以及MySQL的表数据量，检查是否有数据缺失。

```bash

以下执行后开始比对数据量

Linux：
[root@localhost ]# ./oracle_compare_mysql 

Windows：
G:\oracle_mig_mysql>oracle_compare_mysql.exe

```

![image](https://user-images.githubusercontent.com/35289289/190548224-47b35719-81af-4480-903d-afe46325c895.png)


比对完之后，可以通过连接对应的MySQL数据库，查询比对结果
例如：
连接对应MySQL库之后查询

`select * from DATA_COMPARE;`



![image](https://user-images.githubusercontent.com/35289289/190548589-4f8bdfa3-e92e-4631-9d6a-43695e6d6421.png)



## 迁移部分表以及数据


### 仅迁移数据（不创建表结构以及表索引）

以下以Linux环境示例，windows环境操作方式类似


`备注：使用-d命令前，需要确保mysql数据库已经创建了该表的表结构`

`需了解，使用-d命令，会先truncate（清空）目标表的数据再迁移数据`

1、编辑`custom_table.txt`文件输入要迁移数据的表名称

示例如下：

```bash

[root@localhost ]# vi custom_table.txt 

Z_AD_I_SN
Z_D_SN
Z_F_SN 
Z_R_SN 

```

2、开始迁移部分表数据

```bash

Linux下执行命令:

[root@localhost ]# ./oracle_mig_mysql -d

Windows下执行命令:

G:\oracle_mig_mysql>oracle_mig_mysql.exe -d

```


```bash

+---------------------------------------------------+
| Oracle Migrate MySQL Tool                         |
+---------------------------------------------------+
| Powered By: Epoint Infrastructure Research Center |
| Release Date:  2021-12-13                         |
| Support Database: MySQL 5.7 and Oracle 11g higher |
| Tool Version: 1.8.18                              |
+---------------------------------------------------+

Source Database information:
+----------+-------------+-----------------------------------------------------------+
| database | schema_info |                        connect_info                       |
+----------+-------------+-----------------------------------------------------------+
| Oracle   |    admin    | ((HOST=192.168.212.23)(PORT=1521))((SERVICE_NAME=orcl))) |
+----------+-------------+-----------------------------------------------------------+
+---------------------------------------+
| migrate mode                          |
+---------------------------------------+
| Migration Mode:migrate partion tables |
+---------------------------------------+

table for migration:
+-----------------------+
| TABLE_NAME            |
+-----------------------+
| Z_AD_I_SN |
| Z_D_SN         |
| Z_F_SN           |
| Z_R_SN           |
+-----------------------+

Target Database Information:
+----------+-----------------+----------+-----------+----------+
| database |     ip_addr     | port_num | user_name | db_name  |
+----------+-----------------+----------+-----------+----------+
| MySQL    | 192.168.209.2 |   3306   |    root   | temptest |
+----------+-----------------+----------+-----------+----------+
2021-12-23 18:17:00

READY FOR MIGRATING DATABASE ?：(PLEASE INPUT "Y" OR "N" TO CONTINUE)
y --------> 这里输入Y确认是否迁移
GO
START MIGRATING ROW DATA! 2021-12-23 18:17:03.715882 

[Z_AD_I_SN] 插入完成 源表行数：100 目标行数：100  THREAD 1 2021-12-23 18:17:04.077622
[Z_D_SN] 插入完成 源表行数：100 目标行数：100  THREAD 1 2021-12-23 18:17:04.361066
[Z_F_SN] 插入完成 源表行数：100 目标行数：100  THREAD 1 2021-12-23 18:17:04.639245
[Z_R_SN] 插入完成 源表行数：100 目标行数：100  THREAD 1 2021-12-23 18:17:04.900548

```

