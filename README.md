# oracle_to_mysql

![commit](https://img.shields.io/github/last-commit/iverycd/oracle_to_mysql?style=flat-square)
[![tag](https://img.shields.io/github/v/release/iverycd/oracle_to_mysql?display_name=tag)](https://github.com/iverycd/oracle_to_mysql/releases)
![languages](https://img.shields.io/github/languages/top/iverycd/oracle_to_mysql)
[![linux](https://img.shields.io/badge/Linux-support-success?logo=linux)](https://github.com/iverycd/oracle_to_mysql/releases)
[![win](https://img.shields.io/badge/Windows-support-success?logo=windows)](https://github.com/iverycd/oracle_to_mysql/releases)
[![mac](https://img.shields.io/badge/MacOS-support-success?logo=apple)](https://github.com/iverycd/oracle_to_mysql/releases)
## :dizzy:简介

:sparkling_heart:功能特性

:sparkles:支持Linux,Windows,MacOS，Oracle 11.2.0.4及以上，MySQL 5.7,8.0及以上测试通过

:sparkles: 支持在线迁移Oracle到MySQL的表、视图、索引、触发器、外键、自增列、以及部分触发器，不支持存储过程以及函数的迁移

:sparkles: 自动分析源库Oracle字段的长度并适配到MySQL合适的长度，避免MySQL字段长度太长问题 “Row size too large 65535”

:sparkles: 自动处理Oracle触发器+序列形式的自增列适配到MySQL原生自增列

:sparkles: 转储Oracle的函数、存储过程到平面文件以便修改人工翻译适配到MySQL

:sparkles: 后台多进程数据迁移，数据迁移更快

:sparkles: 支持迁移部分表结构以及数据功能

:sparkles: 支持迁移Oracle与MySQL共同存在的列数据，即目标数据库的列字段是源数据库列字段的超集

:sparkles: 记录迁移日志，转储表、视图等DDL对象创建失败的sql语句

:sparkles: 支持Oracle与MySQL表数量快速比对功能


:star:环境要求

- 运行此工具的PC需要能连通源端Oracle以及目标MySQL数据库

- 依赖oracle客户端环境(release已集成instant client)


:camera:平台测试

客户端硬件平台：

| CPU | 内存 | 硬盘 | 备注 |
| :---------------: | :----------------: | :-----------------: | :-----------------: |
| Intel(R) Core(TM) i7-12700 2.10 GHz(12核20线程)| 芝奇皇家戟 DDR4 3600 32G    | 西数SN850        | 迁移工具版本v1.9.28.4


服务端硬件平台

| CPU | 内存 | 硬盘 | 备注 |
| :---------------: | :----------------: | :-----------------: |:-----------------: |
| Intel(R) Xeon(R) E5-2670 v2 2.50GHz(8核16线程)| 三星DDR4 32G      |   INTEL P3700   | Oracle 11.2.0.4 MySQL 5.7.24

源端表结构

```sql
CREATE TABLE TABLE_2000W 
(
LOGFILE_DATA_ID NUMBER,
NAME VARCHAR2(100)
)

```

在以上测试平台迁移Oracle一张2000万的表，迁移耗时`200秒`左右

![image](https://user-images.githubusercontent.com/35289289/192426252-a4631991-2ddc-4e76-951d-a43eed58d65d.png)

充分利用CPU多核心，提高数据迁移效率

![image](https://user-images.githubusercontent.com/35289289/192426838-53eec0cf-dc4d-4731-9217-76b777bd6af2.png)

****

`v1.9.21.1版本及以下`为单线程迁移数据，迁移Oracle一张2000万的表耗时`3000秒`左右

![image](https://user-images.githubusercontent.com/35289289/192427224-b5b4d5d6-1237-4d8a-b655-8079306c3e8e.png)

`v1.9.21.1版本及以下`CPU利用率不高

![image](https://user-images.githubusercontent.com/35289289/192427398-1903ae68-f41b-482a-a6d9-683b01591ad2.png)


:camera:运行概览

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



## :microscope:Dev环境运行
1、oracle客户端环境准备

`Linux`

![image](https://user-images.githubusercontent.com/35289289/192779340-0923d22d-a746-442c-8aae-2add127c9ffd.png)


下载Linux Instant Client（11.2.0.4及以上都行)

https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html


![image](https://user-images.githubusercontent.com/35289289/190542155-1883896f-38af-4693-b72a-577f320cd62e.png)

或者直接下载当前资源库文件[linux_oracle_client.7z](https://github.com/iverycd/oracle_to_mysql/blob/master/linux_oracle_client.7z)

以上解压，并设定oracle client为正确路径的环境变量
```bash
echo "export ORACLE_HOME=/opt/oracle_to_mysql/ora_client/instantclient_11_2"
echo "export LD_LIBRARY_PATH=$ORACLE_HOME:$LD_LIBRARY_PATH"
echo "export PATH=$ORACLE_HOME:$PATH"
```

`MAC`

![image](https://user-images.githubusercontent.com/35289289/192779668-74a4b16e-d49d-4dc3-90ed-f2b946ada797.png)


下载当前资源库文件[mac_oracle_client.7z](https://github.com/iverycd/oracle_to_mysql/blob/master/mac_oracle_client.7z)

将以上目录放在程序相同目录或者自行设定oracle client为正确路径的环境变量

![image](https://user-images.githubusercontent.com/35289289/191474184-4f81036b-5cbc-4a3b-a7dc-3cc257e1cb4b.png)


`Win`

![image](https://user-images.githubusercontent.com/35289289/192779816-f48c0086-b519-4280-ae0f-99c8f6cd9533.png)


下载Windows Instant Client（11.2.0.4及以上都行)

https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html

![image](https://user-images.githubusercontent.com/35289289/190544173-9ba6264f-7df7-46cd-acc2-08b5605ac7bf.png)

或者直接下载当前资源库文件[win_oracle_client.7z](https://github.com/iverycd/oracle_to_mysql/blob/master/win_oracle_client.7z)

解压之后重命名目录名称为`oracle_client`并放到代码同路径，如下（或者自行设定oracle client为正确路径的环境变量）：

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
split_page_size = 10000  # 每个表分页查询的结果集总数
split_process = 16 # 并行执行分页查询的线程数

[mysql]
host = 192.168.209.24
port = 3306
user = root
passwd = Gep
database = temptest
dbchar = utf8mb4
row_batch_size = 10000 # 每次插入到目标表的行数
```

3、全库迁移

```python
python oracle_mig_mysql.py
```


## :computer:如何打包

先修改脚本内oracle_client路径以及Python环境路径

`Linux`打包

:warning:在代码根目录创建hooks目录并编写配置文件

![image](https://user-images.githubusercontent.com/35289289/190604823-81b3f70e-94cb-48ef-89a5-f12271efa979.png)

![image](https://user-images.githubusercontent.com/35289289/190604919-9ff404e8-4df0-480f-8346-6e6f226b5412.png)

```bash
mkdir hooks

vi hooks/hook-prettytable.py
内容如下：
  
from PyInstaller.utils.hooks import collect_data_files, copy_metadata
datas = collect_data_files('prettytable') + copy_metadata('prettytable')

修改脚本为正确路径后，运行打包脚本 `sh pack.sh`
```
 
`MacOS`打包

在程序所在目录运行

```python
修改脚本为正确路径后，运行
sh mac_pack.sh
```

![image](https://user-images.githubusercontent.com/35289289/191474469-f40084a3-7475-407a-8de4-8542db94c274.png)


`Win`

修改脚本为正确路径后，运行bat脚本 `pack.bat`

## :gift:开箱即用的二进制可执行文件

下载[release](https://github.com/iverycd/oracle_to_mysql/releases/)

[![linux](https://img.shields.io/badge/Linux-support-success?logo=linux)](https://github.com/iverycd/oracle_to_mysql/releases)
[![win](https://img.shields.io/badge/Windows-support-success?logo=windows)](https://github.com/iverycd/oracle_to_mysql/releases)
[![mac](https://img.shields.io/badge/MacOS-support-success?logo=apple)](https://github.com/iverycd/oracle_to_mysql/releases)


解压之后即可运行此工具

1、解压，例如将`oracle_mig_mysql.zip`上传到/opt目录

2、在root用户下解压

```bash
[root@localhost root]# cd /opt

[root@localhost root]# unzip oracle_mig_mysql.zip
```




### 全库迁移示例

#### Linux环境


1、进入迁移工具目录

```bash
[root@localhost opt]# cd /opt/oracle_to_mysql
```


2、运行环境变量脚本

```bash
[root@localhost ]# sh env_ora.sh 

```

![image](https://user-images.githubusercontent.com/35289289/191475634-d6788075-bfd6-45af-87e3-0d779b5ddd51.png)


:warning:注意：此步骤仅Linux环境需要，Windows以及MacOS无需执行


3、编辑配置文件 config.ini文件，修改源数据库以及目标数据库信息

```bash

[root@localhost ]# vi config.ini 

[oracle]
host = 192.168.212.23
port = 1521
user = admin
passwd = admin123
service_name = orcl
split_page_size = 10000  # 每个表分页查询的结果集总数
split_process = 16 # 并行执行分页查询的线程数

[mysql]
host = 192.168.209.24
port = 3306
user = root
passwd = Gep
database = temptest
dbchar = utf8mb4
row_batch_size = 10000 # 每次插入到目标表的行数

```


4、执行全库迁移

`须知：如果是通过堡垒机或者是vpn连接的非图形化界面，强烈建议使用后台方式运行此工具，避免数据迁移中断`

```bash

后台执行命令(需要命令后面带-q)：

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



#### MacOS环境


终端内执行即可：

![image](https://user-images.githubusercontent.com/35289289/191476818-e02419d7-b2c6-4522-b103-3dcfe49f209d.png)

![image](https://user-images.githubusercontent.com/35289289/191477136-2d523962-2e2a-46d9-ab3f-93e2bb0bff86.png)


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
split_page_size = 10000  # 每个表分页查询的结果集总数
split_process = 16 # 并行执行分页查询的线程数

[mysql]
host = 192.168.209.24
port = 3306
user = root
passwd = Gep
database = temptest
dbchar = utf8mb4
row_batch_size = 10000 # 每次插入到目标表的行数
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


## :eyes:比对Oracle以及MySQL表数据量

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



## :bookmark:迁移部分表以及数据


### 仅迁移数据（不创建表结构以及表索引）

以下以Linux环境示例，windows环境操作方式类似


:warning:`备注：使用-d命令前，需要确保mysql数据库已经创建了该表的表结构`

:warning:`需了解，使用-d命令，会先truncate（清空）目标表的数据再迁移数据`

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

## :memo:完整命令列表
通过编辑`custom_table.txt` 文件以及配合命令参数可自定义哪些表需要迁移

:diamond_shape_with_a_dot_inside:命令示例：

:one:仅迁移自定义表的表结构以及数据包括索引等约束

```python
python oracle_mig_mysql.py -c
```

:two:仅迁移自定义表的表数据，不包括表结构以及索引等约束

```python
python oracle_mig_mysql.py -d
```

:three:仅迁移自定义表的元数据，即表结构（表定义、视图、索引、触发器自增列等约束），不迁移数据

```python
python oracle_mig_mysql.py -m
```

:four:静默模式,输入-q之后无需在命令行界面键入“y”进行迁移前确认，默认为一键:bangbang:全库迁移:bangbang:

```python
python oracle_mig_mysql.py -q 
```
