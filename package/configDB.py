# -*- coding: UTF-8 -*-
"""
1、执行带参数的ＳＱＬ时，请先用sql语句指定需要输入的条件列表，然后再用tuple/list进行条件批配
２、在格式ＳＱＬ中不需要使用引号指定数据类型，系统会根据输入参数自动识别
３、在输入的值中不需要使用转意函数，系统会自动处理
"""
import sys
import cx_Oracle
import pymysql
from dbutils.pooled_db import PooledDB
# sys.path.append("..")
import readConfig

config = readConfig.ReadConfig()  # 实例化

"""
Config是一些数据库的配置文件,通过调用我们写的readConfig来获取配置文件中对应值
"""
# MySQL read config
mysql_host = config.get_mysql('host')
mysql_port = int(config.get_mysql('port'))
mysql_user = config.get_mysql('user')
mysql_passwd = config.get_mysql('passwd')
mysql_database = config.get_mysql('database')
mysql_dbchar = config.get_mysql('dbchar')

# Oracle read config
oracle_host = config.get_oracle('host')
oracle_port = config.get_oracle('port')
oracle_user = config.get_oracle('user')
oracle_passwd = config.get_oracle('passwd')
oracle_service_name = config.get_oracle('service_name')

MySQLPOOL = PooledDB(
    creator=pymysql,  # 使用链接数据库的模块
    maxconnections=0,  # 连接池允许的最大连接数，0和None表示不限制连接数
    mincached=10,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
    maxcached=0,  # 链接池中最多闲置的链接，0和None不限制
    maxshared=3,
    # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的 threadsafety都为1，所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。
    blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
    maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
    setsession=['SET AUTOCOMMIT=0;','SET foreign_key_checks=0;'],  # 开始会话前执行的命令列表。使用连接池执行dml，这里需要显式指定提交，已测试通过
    ping=0,
    # ping MySQL服务端，检查是否服务可用。
    host=mysql_host,
    port=mysql_port,
    user=mysql_user,
    password=mysql_passwd,
    database=mysql_database,
    charset=mysql_dbchar
)


class OraclePool:

    def __init__(self):
        """
        获得连接池
        :param config:      dict    Oracle连接信息
        """
        self.__pool = self.__get_pool()

    @staticmethod
    def __get_pool():
        """
        :param config:        dict    连接Oracle的信息
        ---------------------------------------------
        以下设置，根据需要进行配置
        maxconnections=6,   # 最大连接数，0或None表示不限制连接数
        mincached=2,        # 初始化时，连接池中至少创建的空闲连接。0表示不创建
        maxcached=5,        # 连接池中最多允许的空闲连接数，很久没有用户访问，连接池释放了一个，由6个变为5个，
                            # 又过了很久，不再释放，因为该项设置的数量为5
        maxshared=0,        # 在多个线程中，最多共享的连接数，Python中无用，会最终设置为0
        blocking=True,      # 没有闲置连接的时候是否等待， True，等待，阻塞住；False，不等待，抛出异常。
        maxusage=None,      # 一个连接最多被使用的次数，None表示无限制
        setession=[],       # 会话之前所执行的命令, 如["set charset ...", "set datestyle ..."]
        ping=0,             # 0  永远不ping
                            # 1，默认值，用到连接时先ping一下服务器
                            # 2, 当cursor被创建时ping
                            # 4, 当SQL语句被执行时ping
                            # 7, 总是先ping
        """
        host, port = oracle_host, oracle_port
        dsn = cx_Oracle.makedsn(host, port, service_name=oracle_service_name)

        pool = PooledDB(
            cx_Oracle,
            mincached=5,
            maxcached=10,
            user=oracle_user,
            password=oracle_passwd,
            dsn=dsn
        )

        return pool

    def __get_conn(self):
        """
        从连接池中获取一个连接，并获取游标。
        :return: conn, cursor
        """
        conn = self.__pool.connection()
        cursor = conn.cursor()

        return conn, cursor

    @staticmethod
    def __reset_conn(conn, cursor):
        """
        把连接放回连接池。
        :return:
        """
        cursor.close()
        conn.close()

    def __execute(self, sql, args=None):
        """
        执行sql语句
        :param sql:     str     sql语句
        :param args:    list    sql语句参数列表
        :param return:  cursor
        """
        conn, cursor = self.__get_conn()

        if args:
            cursor.execute(sql, args)
        else:
            cursor.execute(sql)

        return conn, cursor

    def fetch_all(self, sql, args=None):  # in use
        """
        获取全部结果
        :param sql:     str     sql语句
        :param args:    list    sql语句参数
        :return:        tuple   fetch结果
        """
        conn, cursor = self.__execute(sql, args)
        result = cursor.fetchall()
        self.__reset_conn(conn, cursor)

        return result

    def fetch_one(self, sql, args=None):  # in use
        """
        获取全部结果
        :param sql:     str     sql语句
        :param args:    list    sql语句参数
        :return:        tuple   fetch结果
        """
        conn, cursor = self.__execute(sql, args)
        result = cursor.fetchone()
        self.__reset_conn(conn, cursor)

        return result

    def fetch_many(self, sql, size=None):  # NO USE
        """Fetch several rows"""
        conn, cursor = self.__get_conn()

        if size:
            cursor.execute(sql)
            cx_Oracle.Cursor.fetchmany(size)
        else:
            print('error')

        return cursor

    def execute_sql(self, sql, args=None):  # in use 在连接池中dml操作需要commit
        """
        执行SQL语句。
        :param sql:     str     sql语句
        :param args:    list    sql语句参数
        :return:        tuple   fetch结果
        """
        conn, cursor = self.__execute(sql, args)
        conn.commit()
        self.__reset_conn(conn, cursor)


def __del__(self):
    """
        关闭连接池。
        """
    self.__pool.close()


# 非连接池连接方式以及游标
ora_conn = oracle_user + '/' + oracle_passwd + '@' + oracle_host + ':' + oracle_port + '/' + oracle_service_name

if __name__ == '__main__':
    print(oracle_user, oracle_passwd, oracle_host, oracle_port, oracle_service_name)
    print(mysql_host, mysql_port, mysql_user, mysql_passwd, mysql_database)
