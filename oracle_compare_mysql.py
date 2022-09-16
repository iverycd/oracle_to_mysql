# -*- coding: utf-8 -*-
import datetime
import os
import platform
import sys
import cx_Oracle
import prettytable as pt
import readConfig
import configDB

"""
MySQL database compare data and tables with oracle
oracle_compare_mysql.py v1.5.1 2022-09-16 Linux py 37
精简代码
"""


class Logger(object):
    def __init__(self, filename='default.log', add_flag=True, stream=sys.stdout):
        self.terminal = stream
        self.filename = filename
        self.add_flag = add_flag
        # self.log = open(filename, 'a+')

    def write(self, message):
        if self.add_flag:
            with open(self.filename, 'a+') as log:
                self.terminal.write(message)
                log.write(message)
        else:
            with open(self.filename, 'w') as log:
                self.terminal.write(message)
                log.write(message)

    def flush(self):
        pass


def table_prepare(mysql_cursor):
    mysql_cursor.execute("""drop table if exists data_compare""")
    mysql_cursor.execute("""create table data_compare
(id int ,
source_db_name varchar(100),
source_table_name varchar(100),
source_rows int,
db_type varchar(100),
target_table_name varchar(100),
target_rows int,
is_success varchar(10),
compare_time TIMESTAMP default CURRENT_TIMESTAMP
)""")


def check_db_exist(source_name, target_name,oracle_cursor,mysql_cursor):
    src_result = 0
    trg_result = 0
    try:
        oracle_cursor.execute("""select count(*) from dba_users where username=upper('%s')""" % source_name)
        src_result = oracle_cursor.fetchone()[0]
        mysql_cursor.execute(
            """select count(distinct TABLE_SCHEMA) from information_schema.TABLES where TABLE_SCHEMA='%s' """ % target_name)
        trg_result = mysql_cursor.fetchone()[0]
    except Exception as e:
        print(e)
    return src_result, trg_result


def data_compare_single(sourcedb, target_db,oracle_cursor,mysql_cursor):  # 手动输入源数据库、目标数据库名称，比对全表数据
    table_id = 0
    source_rows = 0
    target_rows = 0
    target_db_name = ''
    target_table_name = ''
    target_view_name = ''
    src_out, trg_out = check_db_exist(sourcedb, target_db,oracle_cursor,mysql_cursor)
    if src_out == 0:
        print(sourcedb, '在源数据库不存在\nEXIT!')
        sys.exit()
    elif trg_out == 0:
        print(target_db, '在目标数据库不存在此模式名\nEXIT!')
        sys.exit()
    else:  # 检查源库、目标库名称是否存在之后，开始比较
        print('开始比较全库数量差异\n源模式名称:', sourcedb, '目标数据库名:', target_db)
        print('----------------------')
        # 先根据oracle的表名查每个表的行数
        oracle_cursor.execute("""select table_name from user_tables where table_name !='DATA_COMPARE'""")
        out_table = oracle_cursor.fetchall()  # 获取所有Oracle表名
        source_table_total = len(out_table)  # 获取Oracle表总数
        mysql_cursor.execute("""select count(*) from information_schema.TABLES where TABLE_SCHEMA='%s' and TABLE_TYPE='BASE TABLE' and 
                    table_name not in ('DATA_COMPARE','MY_MIG_TASK_INFO')""" % target_db)  # 获取MySQL表总数
        target_table_total = mysql_cursor.fetchone()[0]  # 获取MySQL表总数
        print('表总数:' + '源数据库 ' + str(source_table_total) + ' 目标数据库 ' + str(target_table_total))
        for v_out_table in out_table:
            source_table = v_out_table[0]
            table_id += 1
            try:
                oracle_cursor.execute("""select count(*) from %s.\"%s\"""" % (sourcedb, source_table))
                source_rows = oracle_cursor.fetchone()[0]  # 源表行数
            except Exception as e:
                print(e, '获取源表行数失败')
            try:
                target_db_name = target_db
                # 这里判断下源表的名称在目标数据库是否存在
                mysql_cursor.execute("""select count(*) from information_schema.TABLES where TABLE_SCHEMA='%s' and TABLE_TYPE='BASE TABLE' and table_name
                             ='%s'""" % (target_db_name, source_table))
                target_table = mysql_cursor.fetchone()[0]
                if target_table > 0:
                    target_table_name = source_table  # 目标表名称与源库表名称实际相同
                    mysql_cursor.execute(
                        """select count(*) from %s""" % target_table_name)
                    target_rows = mysql_cursor.fetchone()[0]  # 目标表行数
                else:
                    target_table_name = 'TABLE NOT EXIST'  # 目标表不存在就将表命名为TABLE NOT EXIST
                    target_rows = -1
            except Exception as e:
                print(e, ' 在目标数据库查询表' + source_table + '失败')
            try:  # 将以上比对的数据保存在目标库的表里
                if (source_rows != target_rows) or (source_table.upper() != target_table_name.upper()):
                    is_success = 'N'
                else:
                    is_success = 'Y'
                mysql_cursor.execute("""insert into data_compare
                                                        (id,
                                                                    source_db_name,
                                                                    source_table_name,
                                                                    source_rows,
                                                                    db_type,
                                                                    target_table_name,
                                                                    target_rows,
                                                                    is_success
                                                                    ) values(%s,'%s','%s',%s,'%s','%s',%s,'%s')""" % (
                    table_id, sourcedb.upper(), source_table.upper(), source_rows,
                    'TABLE',
                    target_table_name.upper(),
                    target_rows, is_success.upper()))
                mysql_cursor.execute('commit')
            except Exception as e:
                print(e, '数据比对结果保存在目标表失败')
                mysql_cursor.execute('rollback')
        # 视图比较
        try:
            oracle_cursor.execute("""select view_name from user_views """)  # oracle所有视图名称
            out_view = oracle_cursor.fetchall()
            source_view_total = len(out_view)  # Oracle所有视图数量
        except Exception as e:
            print(e, '获取源数据库视图失败')
        try:
            mysql_cursor.execute(
                """select count(*) from information_schema.TABLES where TABLE_SCHEMA='%s' and TABLE_TYPE='VIEW'""" % target_db_name)  # mysql视图总数
            target_view_total = mysql_cursor.fetchone()[0]
        except Exception as e:
            print(e, '获取目标视图总数失败')
        print('视图总数:' + '源数据库 ' + str(source_view_total) + ' 目标数据库 ' + str(target_view_total))
        for v_out_view in out_view:
            source_view_name = v_out_view[0]
            table_id += 1
            try:
                target_db_name = target_db
                mysql_cursor.execute(
                    """select count(*) from information_schema.TABLES where TABLE_SCHEMA='%s' and TABLE_TYPE='VIEW' and table_name='%s'""" % (
                        target_db_name, source_view_name))
                target_view = mysql_cursor.fetchone()[0]  # 目标视图名称
                if target_view == 0:
                    target_view_name = 'NOT EXISTS VIEW'
                else:
                    target_view_name = source_view_name
            except Exception as e:
                print(e, ' 在目标数据库查询视图失败', target_view_name)
            if source_view_name.upper() != str(target_view_name).upper():
                is_success = 'N'
            else:
                is_success = 'Y'
            try:  # 将以上比对的数据保存在目标库的表里
                mysql_cursor.execute("""insert into data_compare
                                                        (id,
                                                                    source_db_name,
                                                                    source_table_name,
                                                                    source_rows,
                                                                    db_type,
                                                                    target_table_name,
                                                                    target_rows,
                                                                    is_success
                                                                    ) values(%s,'%s','%s',%s,'%s','%s',%s,'%s')""" % (
                    table_id, sourcedb.upper(), source_view_name.upper(), 0,
                    'VIEW',
                    target_view_name.upper(),
                    0, is_success.upper()))
                mysql_cursor.execute('commit')
            except Exception as e:
                print(e, '数据比对结果保存在目标表失败')
                mysql_cursor.execute('rollback')


def main():
    # 创建日志文件夹
    log_path = ''
    theTime = datetime.datetime.now()
    theTime = str(theTime)
    date_split = theTime.strip().split(' ')
    date_today = date_split[0].replace('-', '_')
    date_clock = date_split[-1]
    date_clock = date_clock.strip().split('.')[0]
    date_clock = date_clock.replace(':', '_')
    date_show = date_today + '_' + date_clock
    if platform.system().upper() == 'WINDOWS':
        # 设置Oracle客户端的环境变量
        oracle_home = os.path.dirname(os.path.realpath(sys.argv[0])) + '\\oracle_client'
        os.environ['oracle_home'] = oracle_home  # 设置环境变量，如果当前系统存在多个Oracle客户端，需要设置下此次运行的客户端路径
        os.environ['path'] = oracle_home + ';' + os.environ['path']  # 设置环境变量，Oracle客户端可执行文件路径
        os.environ['LANG'] = 'zh_CN.UTF8'  # 需要设置语言环境变量，在部分机器上可能会有乱码
        os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'  # 需要设置语言环境变量，在部分机器上可能会有乱码
        log_path = "mig_log" + '\\' + date_show + '\\'
        if not os.path.isdir(log_path):
            os.makedirs(log_path)
    elif platform.system().upper() == 'LINUX':
        log_path = "mig_log" + '/' + date_show + '/'
        if not os.path.isdir(log_path):
            os.makedirs(log_path)
    else:
        print('can not create dir,please run on win or linux!\n')
    config = readConfig.ReadConfig()  # 实例化
    mysql_conn = configDB.MySQLPOOL.connection()
    mysql_cursor = mysql_conn.cursor()  # MySQL连接池
    mysql_database = config.get_mysql('database')
    mysql_dbchar = config.get_mysql('dbchar')
    # Oracle read config
    oracle_host = config.get_oracle('host')
    oracle_port = config.get_oracle('port')
    oracle_user = config.get_oracle('user')
    oracle_passwd = config.get_oracle('passwd')
    oracle_service_name = config.get_oracle('service_name')

    oracle_conn = cx_Oracle.connect(
        oracle_user + '/' + oracle_passwd + '@' + oracle_host + ':' + oracle_port + '/' + oracle_service_name)
    oracle_cursor = oracle_conn.cursor()
    sys.stdout = Logger(log_path + "\\compare.log", sys.stdout)
    table_prepare(mysql_cursor)
    data_compare_single(oracle_user, mysql_database,oracle_cursor,mysql_cursor)
    print('表结果比较如下:')
    mysql_cursor.execute("""select * from DATA_COMPARE""")
    data_compare_out = mysql_cursor.fetchall()
    tb = pt.PrettyTable()
    tb.field_names = ['id', 'source_db_name', 'source_table_name', 'source_rows', 'type', 'target_table_name',
                      'target_rows', 'is_success', 'compare_time']
    tb.align['id'] = 'l'
    tb.align['source_db_name'] = 'l'
    tb.align['source_object_name'] = 'l'
    tb.align['source_rows'] = 'l'
    tb.align['target_object_type'] = 'l'
    tb.align['target_object_name'] = 'l'
    tb.align['target_rows'] = 'l'
    tb.align['is_success'] = 'l'
    for v_data_compare_out in data_compare_out:
        tb.add_row(list(v_data_compare_out))
    print(tb)
    print('数据比较已结束，请查看目标' + mysql_database.upper() + '.' + 'DATA_COMPARE获取详细信息')
    mysql_cursor.close()
    oracle_conn.close()


if __name__ == "__main__":
    main()
