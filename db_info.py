import csv
import datetime
import logging
import os
import re
import sys
import time
import traceback
import configDB
import prettytable
import sql_format
import platform
from HTMLTable import (HTMLTable)
import ctypes

if platform.system().upper() == 'WINDOWS':
    exepath = os.path.dirname(os.path.realpath(sys.argv[0])) + '\\'
    # 设置Oracle客户端的环境变量
    oracle_home = os.path.dirname(os.path.realpath(sys.argv[0])) + '\\oracle_client'
    os.environ['oracle_home'] = oracle_home  # 设置环境变量，如果当前系统存在多个Oracle客户端，需要设置下此次运行的客户端路径
    os.environ['path'] = oracle_home + ';' + os.environ['path']  # 设置环境变量，Oracle客户端可执行文件路径
    os.environ['LANG'] = 'zh_CN.UTF8'  # 需要设置语言环境变量，在部分机器上可能会有乱码
    os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'  # 需要设置语言环境变量，在部分机器上可能会有乱码
elif platform.system().upper() == 'LINUX' or platform.system().upper() == 'DARWIN':
    exepath = os.path.dirname(os.path.abspath(__file__)) + '/'
    oracle_home = os.path.dirname(os.path.realpath(sys.argv[0])) + '/oracle_client'
    os.environ['ORACLE_HOME'] = oracle_home
    if platform.system().upper() == 'LINUX':
        if 'oracle_client' not in os.environ['LD_LIBRARY_PATH']:
            print('LD_LIBRARY_PATH->', os.environ['LD_LIBRARY_PATH'])
            print('Please check oracle_client is setted correct path\n')
            print('You can run command "sh env_ora.sh && source run_env" and try it again')
            sys.exit(0)
else:
    print('set oracle client failed\n')


class DbMetadata(object):
    def __init__(self):
        try:
            self.oracle_cursor = configDB.OraclePool()  # Oracle连接池
            self.mysql_cursor = configDB.MySQLPOOL.connection().cursor()  # MySQL连接池
            self.oracle_info = self.oracle_cursor._OraclePool__pool._kwargs
            self.mysql_info = self.mysql_cursor._con._kwargs
            if str(self.mysql_cursor._con._con.server_version)[:1] == '8':
                self.mysql_cursor._con._setsession_sql = ['SET AUTOCOMMIT=0;', 'SET foreign_key_checks=0;',
                                                          'set session sql_require_primary_key=OFF']

        except Exception as e:
            print('connect database failed please check oracle client is correct or network is ok\n', e)

    def tbl_columns(self, table_name, fix_mode='N'):
        # 获取Oracle的列字段类型以及字段长度以及映射数据类型到MySQL的规则
        col_len = 0
        output_table_col = []
        default_str = ''
        sql = """SELECT A.COLUMN_NAME, A.DATA_TYPE, A.CHAR_LENGTH, 
        case when A.DATA_PRECISION is null then -1 else  A.DATA_PRECISION end DATA_PRECISION, 
        case when A.DATA_SCALE is null then -1 when A.DATA_SCALE >30 then least(A.DATA_PRECISION,30)-1 else  A.DATA_SCALE end DATA_SCALE, 
         case when A.NULLABLE ='Y' THEN 'True' ELSE 'False' END as isnull, B.COMMENTS,A.DATA_DEFAULT,
         case when a.AVG_COL_LEN is null then -1 else a.AVG_COL_LEN end AVG_COL_LEN
                FROM USER_TAB_COLUMNS A LEFT JOIN USER_COL_COMMENTS B 
                ON A.TABLE_NAME=B.TABLE_NAME AND A.COLUMN_NAME=B.COLUMN_NAME 
                WHERE A.TABLE_NAME='%s' ORDER BY COLUMN_ID ASC""" % table_name
        try:
            output_table_col = self.oracle_cursor.fetch_all(sql)
        except Exception as e:
            print(e, 'get table column failed')
        result = []
        exclude_default_str = ['SYSDATE', 'SYS_GUID', 'USER']
        # primary_key = table_primary(table_name)
        for column in output_table_col:  # 按照游标行遍历字段
            '''
            result.append({'column_name': column[0],
                           'data_type': column[1],
                           'data_length': column[2],
                           'data_precision': column[3],
                           'data_scale': column[4],
                           'isnull': column[5],
                           'comments': column[6],
                           'data_default': column[7],
                           'avg_col_len': column[8]
                           })
            '''
            default_str = column[7]
            if default_str is not None:
                default_str = str(default_str).upper()
                # 去掉默认值当中的空格
                default_str = default_str.replace(' ', '')
                #  去除oracle列中默认值的括号
                if '(' in default_str or ')' in default_str:
                    default_str = default_str.replace('(', '')
                    default_str = default_str.replace(')', '')
                # 去掉oracle列有函数的默认值
                if default_str in exclude_default_str:
                    default_str = ''
            # 对游标cur_tbl_columns中每行的column[0-8]各字段进行层级判断
            # 字符类型映射规则，字符串类型映射为MySQL类型varchar(n),注意NVARCHAR2(n),n是存储的字符而不是字节
            if column[1] == 'VARCHAR2' or column[1] == 'NVARCHAR2':
                tbl_name = table_name
                col_name = column[0]
                col_name = '"' + col_name + '"'
                col_len = int(column[2])  # 获取的是字段长度括号内的大小，如varchar2(50),长度为50，nvarchar2(100)，长度是100
                # 如果在MySQL创建表Row size too large，在创建表遇到异常之后会使用如下获取oracle字段的实际长度
                if fix_mode == 'FIX':
                    try:
                        col_len = \
                            self.oracle_cursor.fetch_one(
                                """select nvl(max(length(%s)),0)  from \"%s\"""" % (col_name, tbl_name))[0]
                    except Exception as e:
                        print(e, 'get actual column length failed')
                    if col_len == 0:  # 如果某些表没有数据。默认长度为100
                        col_len = 100
                    else:
                        col_len = round(int(col_len) * 1.5)  # 查出的实际长度乘以1.5作为MySQL的长度
                #  由于MySQL创建表的时候除了大字段，所有列长度不能大于64k，为了转换方便，如果Oracle字符串长度大于等于1000映射为MySQL的tinytext
                #  由于MySQL大字段不能有默认值，所以这里的默认值都统一为null
                if column[2] >= 10000:  # 此处设定了一个大值，目的是对此条件不生效，即这条规则当前弃用
                    result.append({'fieldname': column[0],  # 如下为字段的属性值
                                   'type': 'TINYTEXT',  # 列字段类型以及长度范围
                                   'primary': column[0],  # 如果有主键字段返回true，否则false
                                   'default': 'null',  # 字段默认值
                                   'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                   'comment': column[6]
                                   }
                                  )

                #  Oracle字符串小于1000的映射为MySQL的varchar，然后下面再对字符串的默认值做判断
                elif column[7] is None:  # 对Oracle字符串类型默认值为null的判断
                    result.append({'fieldname': column[0],  # 如下为字段的属性值
                                   'type': 'VARCHAR' + '(' + str(col_len) + ')',  # 列字段类型以及长度范围
                                   'primary': column[0],  # 如果有主键字段返回true，否则false
                                   'default': default_str,  # 字段默认值
                                   'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                   'comment': column[6]
                                   }
                                  )
                else:  # 其余情况的默认值，MySQL保持默认不变
                    result.append({'fieldname': column[0],  # 如下为字段的属性值
                                   'type': 'VARCHAR' + '(' + str(col_len) + ')',  # 列字段类型以及长度范围
                                   'primary': column[0],  # 如果有主键字段返回true，否则false
                                   'default': default_str,  # 字段默认值
                                   'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                   'comment': column[6]
                                   }
                                  )
            # 字符类型映射规则，字符串类型映射为MySQL类型char(n)
            elif column[1] == 'CHAR' or column[1] == 'NCHAR':
                result.append({'fieldname': column[0],  # 如下为字段的属性值
                               'type': 'CHAR' + '(' + str(column[2]) + ')',  # 列字段类型以及长度范围
                               'primary': column[0],  # 如果有主键字段返回true，否则false
                               'default': default_str,  # 字段默认值
                               'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                               'comment': column[6]
                               }
                              )
            elif column[1] == 'UROWID':
                result.append({'fieldname': column[0],  # 如下为字段的属性值
                               'type': 'VARCHAR' + '(' + str(column[2]) + ')',  # 列字段类型以及长度范围
                               'primary': column[0],  # 如果有主键字段返回true，否则false
                               'default': default_str,  # 字段默认值
                               'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                               'comment': column[6]
                               }
                              )

            # 时间日期类型映射规则，Oracle date类型映射为MySQL类型datetime
            elif column[1] == 'DATE' or column[1] == 'TIMESTAMP(6)' or column[1] == 'TIMESTAMP(0)':
                # Oracle 默认值sysdate映射到MySQL默认值current_timestamp
                if column[7] == 'sysdate' or column[7] == '( (SYSDATE) )':
                    result.append({'fieldname': column[0],  # 如下为字段的属性值
                                   'type': 'DATETIME',  # 列字段类型以及长度范围
                                   'primary': column[0],  # 如果有主键字段返回true，否则false
                                   'default': 'current_timestamp()',  # 字段默认值
                                   'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                   'comment': column[6]
                                   }
                                  )
                # 其他时间日期默认值保持不变(原模原样对应)
                else:
                    result.append({'fieldname': column[0],  # 如下为字段的属性值
                                   'type': 'DATETIME',  # 列字段类型以及长度范围
                                   'primary': column[0],  # 如果有主键字段返回true，否则false
                                   'default': '',  # 字段默认值
                                   'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                   'comment': column[6]
                                   }
                                  )

            # 数值类型映射规则，判断Oracle number类型是否是浮点，是否是整数，转为MySQL的int或者decimal。下面分了3种情况区分整数与浮点
            # column[n] == -1,即DATA_PRECISION，DATA_SCALE，AVG_COL_LEN为null，仅在如下if条件判断是否为空
            elif column[1] == 'NUMBER':
                # 场景1:浮点类型判断，如number(5,2)映射为MySQL的DECIMAL(5,2)
                # Oracle number(m,n) -> MySQL decimal(m,n)
                if column[3] > 0 and column[4] > 0:
                    result.append({'fieldname': column[0],
                                   'type': 'DECIMAL' + '(' + str(column[3]) + ',' + str(column[4]) + ')',  # 列字段类型以及长度范围
                                   'primary': column[0],  # 如果有主键字段返回true，否则false
                                   'default': column[7],  # 字段默认值
                                   'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                   'comment': column[6]
                                   }
                                  )
                # 场景2:整数类型以及平均字段长度判断，如number(20,0)，如果AVG_COL_LEN比较大，映射为MySQL的bigint
                # column[8] >= 6 ,Oracle number(m,0) -> MySQL bigint
                elif column[3] > 0 and column[4] == 0 and column[8] >= 6:
                    # number类型的默认值有3种情况，一种是null，一种是字符串值为null，剩余其他类型只提取默认值数字部分
                    if column[7] is None:  # 对Oracle number字段类型默认值为null的判断
                        result.append({'fieldname': column[0],
                                       'type': 'BIGINT',  # 列字段类型以及长度范围
                                       'primary': column[0],  # 如果有主键字段返回true，否则false
                                       'default': column[7],  # 字段默认值,设为原值null
                                       'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                       'comment': column[6]
                                       }
                                      )
                    elif column[7].upper().startswith('NULL'):  # 对默认值的字符串值等于'null'的做判断
                        result.append({'fieldname': column[0],
                                       'type': 'BIGINT',  # 列字段类型以及长度范围
                                       'primary': column[0],  # 如果有主键字段返回true，否则false
                                       'default': column[7],  # 字段默认值,设为原值null
                                       'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                       'comment': column[6]
                                       }
                                      )
                    else:  # 其余情况通过正则只提取数字部分，即去掉原Oracle中有括号的默认值
                        result.append({'fieldname': column[0],
                                       'type': 'BIGINT',  # 列字段类型以及长度范围
                                       'primary': column[0],  # 如果有主键字段返回true，否则false
                                       'default': '' if column[7].upper() == """''""" else
                                       re.findall(r'\b\d+\b', column[7])[0],
                                       # 字段默认值如果是''包围则将MySQL默认值调整为null，其余单引号包围去掉括号，仅提取数字
                                       'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                       'comment': column[6]
                                       }
                                      )

                # 场景3:整数类型以及平均字段长度判断，如number(10,0)，如果AVG_COL_LEN比较小，映射为MySQL的INT
                # column[8] < 6 ,Oracle number(m,0) -> MySQL bigint
                elif column[3] > 0 and column[4] == 0 and column[8] < 6:
                    if column[7] is None:  # 对Oracle number字段类型默认值为null的判断
                        result.append({'fieldname': column[0],
                                       'type': 'INT',  # 列字段类型以及长度范围
                                       'primary': column[0],  # 如果有主键字段返回true，否则false
                                       'default': column[7],  # 字段默认值,设为原值null
                                       'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                       'comment': column[6]
                                       }
                                      )
                    elif column[7].upper().startswith('NULL'):  # 对默认值的字符串值等于'null'的做判断
                        result.append({'fieldname': column[0],
                                       'type': 'INT',  # 列字段类型以及长度范围
                                       'primary': column[0],  # 如果有主键字段返回true，否则false
                                       'default': column[7],  # 字段默认值,设为原值null
                                       'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                       'comment': column[6]
                                       }
                                      )
                    elif column[7].upper() == '':  # 对默认值的字符串值等于''的做判断
                        result.append({'fieldname': column[0],
                                       'type': 'INT',  # 列字段类型以及长度范围
                                       'primary': column[0],  # 如果有主键字段返回true，否则false
                                       'default': column[7],  # 字段默认值,设为原值null
                                       'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                       'comment': column[6]
                                       }
                                      )
                    else:  # 其余情况通过正则只提取数字部分，即去掉原Oracle中有括号的默认值
                        result.append({'fieldname': column[0],
                                       'type': 'INT',  # 列字段类型以及长度范围
                                       'primary': column[0],  # 如果有主键字段返回true，否则false
                                       'default': '' if column[7].upper() == """''""" else
                                       re.findall(r'\b\d+\b', column[7])[0],
                                       # 字段默认值如果是''包围则将MySQL默认值调整为null，其余单引号包围去掉括号，仅提取数字
                                       'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                       'comment': column[6]
                                       }
                                      )

                # 场景4:无括号包围的number整数类型以及长度判断，如id number,若AVG_COL_LEN比较大，映射为MySQL的bigint
                # column[8] >= 6 ,Oracle number -> MySQL bigint
                elif column[3] == -1 and column[4] == -1 and column[8] >= 6:
                    if column[7] is None:  # 对Oracle number字段类型默认值为null的判断
                        result.append({'fieldname': column[0],
                                       'type': 'BIGINT',  # 列字段类型以及长度范围
                                       'primary': column[0],  # 如果有主键字段返回true，否则false
                                       'default': column[7],  # 字段默认值,设为原值null
                                       'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                       'comment': column[6]
                                       }
                                      )
                    elif column[7].upper().startswith('NULL'):  # 对默认值的字符串值等于'null'的做判断
                        result.append({'fieldname': column[0],
                                       'type': 'BIGINT',  # 列字段类型以及长度范围
                                       'primary': column[0],  # 如果有主键字段返回true，否则false
                                       'default': column[7],  # 字段默认值,设为原值null
                                       'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                       'comment': column[6]
                                       }
                                      )
                    elif column[7].upper() == '':  # 对默认值的字符串值等于''的做判断
                        result.append({'fieldname': column[0],
                                       'type': 'BIGINT',  # 列字段类型以及长度范围
                                       'primary': column[0],  # 如果有主键字段返回true，否则false
                                       'default': column[7],  # 字段默认值,设为原值null
                                       'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                       'comment': column[6]
                                       }
                                      )
                    else:  # 其余情况通过正则只提取数字部分，即去掉原Oracle中有括号的默认值
                        result.append({'fieldname': column[0],
                                       'type': 'BIGINT',  # 列字段类型以及长度范围
                                       'primary': column[0],  # 如果有主键字段返回true，否则false
                                       'default': '' if column[7].upper() == """''""" else
                                       re.findall(r'\b\d+\b', column[7])[0],
                                       # 字段默认值如果是''包围则将MySQL默认值调整为null，其余单引号包围去掉括号，仅提取数字
                                       'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                       'comment': column[6]
                                       }
                                      )

                # 场景5:无括号包围的number整数类型判断，如id number,若AVG_COL_LEN比较小，映射为MySQL的INT
                # column[8] < 6 ,Oracle number -> MySQL int
                elif column[3] == -1 and column[4] == -1 and column[8] < 6:
                    if column[7] is None:  # 对默认值是否为null的判断
                        result.append({'fieldname': column[0],
                                       'type': 'INT',  # 列字段类型以及长度范围
                                       'primary': column[0],  # 如果有主键字段返回true，否则false
                                       'default': column[7],  # 字段默认值,设为原值null
                                       'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                       'comment': column[6]
                                       }
                                      )
                    elif column[7].upper().startswith('NULL'):  # 对数据库中默认值字符串为'null'的判断
                        result.append({'fieldname': column[0],
                                       'type': 'INT',  # 列字段类型以及长度范围
                                       'primary': column[0],  # 如果有主键字段返回true，否则false
                                       'default': column[7],  # 字段默认值,设为原值null
                                       'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                       'comment': column[6]
                                       }
                                      )
                    else:  # 其余情况number字段类型正则提取默认值数字部分
                        result.append({'fieldname': column[0],
                                       'type': 'INT',  # 列字段类型以及长度范围
                                       'primary': column[0],  # 如果有主键字段返回true，否则false
                                       'default': '' if column[7].upper() == """''""" else
                                       re.findall(r'\b\d+\b', column[7])[0],
                                       # 字段默认值如果是''包围则将MySQL默认值调整为null，其余单引号包围去掉括号，仅提取数字
                                       'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                       'comment': column[6]
                                       }
                                      )

                # 场景6:int整数类型判断，如id int,(oracle的int会自动转为number),若AVG_COL_LEN比较大，映射为MySQL的bigint
                elif column[3] == -1 and column[4] == 0 and column[8] >= 6:
                    if column[7] is None:  # 对默认值是否为null的判断
                        result.append({'fieldname': column[0],
                                       'type': 'BIGINT',  # 列字段类型以及长度范围
                                       'primary': column[0],  # 如果有主键字段返回true，否则false
                                       'default': column[7],  # 字段默认值,设为原值null
                                       'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                       'comment': column[6]
                                       }
                                      )
                    elif column[7].upper().startswith('NULL'):  # 数据库中字段类型默认值为字符串'null'的判断
                        result.append({'fieldname': column[0],
                                       'type': 'BIGINT',  # 列字段类型以及长度范围
                                       'primary': column[0],  # 如果有主键字段返回true，否则false
                                       'default': column[7],  # 字段默认值,设为原值null
                                       'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                       'comment': column[6]
                                       }
                                      )
                    else:  # 其余情况number字段类型正则提取默认值数字部分
                        result.append({'fieldname': column[0],
                                       'type': 'BIGINT',  # 列字段类型以及长度范围
                                       'primary': column[0],  # 如果有主键字段返回true，否则false
                                       'default': '' if column[7].upper() == """''""" else
                                       re.findall(r'\b\d+\b', column[7])[0],
                                       # 字段默认值如果是''包围则将MySQL默认值调整为null，其余单引号包围去掉括号，仅提取数字
                                       'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                       'comment': column[6]
                                       }
                                      )

                # 场景7:int整数类型判断，如id int,(oracle的int会自动转为number)若AVG_COL_LEN比较小，映射为MySQL的INT
                elif column[3] == -1 and column[4] == 0 and column[8] < 6:
                    if column[7] is None:  # 对默认值是否为null的判断
                        result.append({'fieldname': column[0],
                                       'type': 'INT',  # 列字段类型以及长度范围
                                       'primary': column[0],  # 如果有主键字段返回true，否则false
                                       'default': column[7],  # 字段默认值,设为原值null
                                       'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                       'comment': column[6]
                                       }
                                      )
                    elif column[7].upper().startswith('NULL'):  # 数据库中字段类型默认值为字符串'null'的判断
                        result.append({'fieldname': column[0],
                                       'type': 'INT',  # 列字段类型以及长度范围
                                       'primary': column[0],  # 如果有主键字段返回true，否则false
                                       'default': column[7],  # 字段默认值,设为原值null
                                       'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                       'comment': column[6]
                                       }
                                      )
                    else:  # 其余情况number字段类型正则提取默认值数字部分
                        result.append({'fieldname': column[0],
                                       'type': 'INT',  # 列字段类型以及长度范围
                                       'primary': column[0],  # 如果有主键字段返回true，否则false
                                       'default': '' if column[7].upper() == """''""" else
                                       re.findall(r'\b\d+\b', column[7])[0],
                                       # 字段默认值如果是''包围则将MySQL默认值调整为null，其余单引号包围去掉括号，仅提取数字
                                       'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                       'comment': column[6]
                                       }
                                      )
            # 大字段映射规则，文本类型大字段映射为MySQL类型longtext,大字段不能有默认值，这里统一为null
            elif column[1] == 'CLOB' or column[1] == 'NCLOB' or column[1] == 'LONG':
                result.append({'fieldname': column[0],  # 如下为字段的属性值
                               'type': 'LONGTEXT',  # 列字段类型以及长度范围
                               'primary': column[0],  # 如果有主键字段返回true，否则false
                               'default': '',  # 字段默认值
                               'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                               'comment': column[6]
                               }
                              )
            # 大字段映射规则，16进制类型大字段映射为MySQL类型longblob
            elif column[1] == 'BLOB' or column[1] == 'RAW' or column[1] == 'LONG RAW':
                result.append({'fieldname': column[0],  # 如下为字段的属性值
                               'type': 'LONGBLOB',  # 列字段类型以及长度范围
                               'primary': column[0],  # 如果有主键字段返回true，否则false
                               'default': '',  # 字段默认值
                               'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                               'comment': column[6]
                               }
                              )
            else:
                result.append({'fieldname': column[0],  # 如果是非大字段类型，通过括号加上字段类型长度范围
                               'type': column[1] + '(' + str(column[2]) + ')',  # 列字段类型以及长度范围
                               'primary': column[0],  # 如果有主键字段返回true，否则false
                               'default': column[7],  # 字段默认值
                               'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                               'comment': column[6]
                               }

                              )
        return result

    def get_info(self, run_method, mode, log_path, version):
        # 打印连接信息
        k = prettytable.PrettyTable(field_names=["Oracle Migrate MySQL Tool"])
        k.align["Oracle Migrate MySQL Tool"] = "l"
        k.padding_width = 1  # 填充宽度
        k.add_row(["MySQL 5.7 and Oracle 11g higher Support"])
        k.add_row(["Version " + version])
        k.add_row(["Powered By DBA Group of Infrastructure Research Center"])
        print(k.get_string(sortby="Oracle Migrate MySQL Tool", reversesort=False))
        print('\nSource Database information:')
        # print source connect info
        x = prettytable.PrettyTable(field_names=["database", "schema_info", "connect_info"])
        x.align["database"] = "l"  # 以序号字段左对齐
        x.padding_width = 1  # 填充宽度
        x.add_row(['Oracle', str(self.oracle_info['user']),
                   str(self.oracle_info['dsn']).replace('DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)', '').replace(
                       'CONNECT_DATA=',
                       '')])
        # 表排序
        print(x.get_string(sortby="database", reversesort=False))
        if run_method == 1:
            # print database mode
            k = prettytable.PrettyTable(field_names=["migrate mode"])
            k.align["migrate mode"] = "l"
            k.padding_width = 1  # 填充宽度
            k.add_row(["Migration Mode:migrate partion tables"])
            print(k.get_string(sortby="migrate mode", reversesort=False))
            print('\ntable for migration:')
            zz = prettytable.PrettyTable()
            zz.field_names = ['TABLE_NAME']
            zz.align['TABLE_NAME'] = 'l'
            with open(log_path + "table.txt", "r") as f:  # 打开文件
                for line in f:
                    if len(line) > 0:
                        zz.add_row([line.strip('\n').upper()])
                print(zz)
                # print(line.strip('\n').upper())
        else:
            # print database mode
            k = prettytable.PrettyTable(field_names=["migrate mode"])
            k.align["migrate mode"] = "l"
            k.padding_width = 1  # 填充宽度
            if run_method == 2:
                k.add_row(["Migration Mode:create metadata only"])
            else:
                k.add_row(["Migration Mode:full database"])
            print(k.get_string(sortby="migrate mode", reversesort=False))
            try:
                source_table_count = self.oracle_cursor.fetch_one("""select count(*) from user_tables""")[0]
                source_view_count = self.oracle_cursor.fetch_one("""select count(*) from user_views""")[0]
                source_trigger_count = self.oracle_cursor.fetch_one(
                    """select count(*) from user_triggers where TRIGGER_NAME not like 'BIN$%'""")[0]
                source_procedure_count = self.oracle_cursor.fetch_one(
                    """select count(*) from USER_PROCEDURES where OBJECT_TYPE='PROCEDURE' and OBJECT_NAME  not like 'BIN$%'""")[
                    0]
                source_function_count = self.oracle_cursor.fetch_one(
                    """select count(*) from USER_PROCEDURES where OBJECT_TYPE='FUNCTION' and OBJECT_NAME  not like 'BIN$%'""")[
                    0]
                source_package_count = self.oracle_cursor.fetch_one(
                    """select count(*) from USER_PROCEDURES where OBJECT_TYPE='PACKAGE' and OBJECT_NAME  not like 'BIN$%'""")[
                    0]
            except Exception as e:
                print(e)
                source_table_count, source_view_count, source_trigger_count, source_procedure_count, source_function_count, source_package_count = 0, 0, 0, 0, 0, 0
            # print sourcedatabase info
            print('Source Database Information:')
            x = prettytable.PrettyTable(
                field_names=["tables", "views", "triggers", "procedures", "functions", "packages"])
            x.align["tables"] = "l"  # 以序号字段左对齐
            x.padding_width = 1  # 填充宽度
            x.add_row(
                [str(source_table_count), str(source_view_count), str(source_trigger_count),
                 str(source_procedure_count),
                 str(source_function_count), str(source_package_count)])
            # 表排序
            print(x.get_string(sortby="tables", reversesort=False))
        # print target connect info
        print('\nTarget Database Information:')
        x = prettytable.PrettyTable(field_names=["database", "ip_addr", "port_num", "user_name", "db_name"])
        x.align["database"] = "l"  # 以序号字段左对齐
        x.padding_width = 1  # 填充宽度
        x.add_row(['MySQL', str(self.mysql_info['host']), str(self.mysql_info['port']), str(self.mysql_info['user']),
                   str(self.mysql_info['database'])])
        # 表排序
        print(x.get_string(sortby="database", reversesort=False))
        print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        # 静默方式下无需手动输入Y再迁移
        if mode == 1:
            is_continue = 'Y'
            print('QUITE MODE ')
        else:
            is_continue = input('\nREADY FOR MIGRATING DATABASE ?:(PLEASE INPUT "Y" OR "N" TO CONTINUE)\n')
        if is_continue == 'Y' or is_continue == 'y':
            print('GO')  # continue
            if platform.system().upper() == 'WINDOWS':
                kernel32 = ctypes.windll.kernel32
                kernel32.SetConsoleMode(kernel32.GetStdHandle(-10), 128)
        else:
            sys.exit()
        # 创建迁移任务表，用来统计表插入以及完成的时间
        try:
            self.mysql_cursor.execute("""drop table if exists my_mig_task_info""")
            self.mysql_cursor.execute(
                """create table my_mig_task_info(table_name varchar(500),task_start_time datetime(3) default current_timestamp(3),  task_end_time datetime(3) default current_timestamp(3),thread int,run_time decimal(30,6),source_table_rows bigint default 0,target_table_rows bigint default 0, is_success varchar(100) default '',run_status varchar(10) default '',type varchar(100) default 'TABLE',detail varchar(100) default '')""")
        except Exception as e:
            print(e)

    def run_info(self, exepath, log_path, mig_start_time, mig_end_time, all_table_count, list_success_table,
                 ddl_failed_table_result,
                 all_constraints_count,
                 all_constraints_success_count,
                 function_based_index_count, constraint_failed_count, all_fk_count, all_fk_success_count,
                 foreignkey_failed_count, all_inc_col_success_count, all_inc_col_failed_count, normal_trigger_count,
                 trigger_success_count, oracle_autocol_total,
                 trigger_failed_count, all_view_count, all_view_success_count, all_view_failed_count,
                 view_failed_result):
        # Oracle源表信息
        oracle_tab_count = all_table_count  # oracle要迁移的表总数
        oracle_view_count = all_view_count  # oracle要创建的视图总数
        oracle_constraint_count = all_constraints_count + function_based_index_count  # oracle的约束以及索引总数
        oracle_fk_count = all_fk_count  # oracle外键总数
        # MySQL迁移计数
        self.mysql_cursor.execute("""select database()""")
        mysql_database_name = self.mysql_cursor.fetchone()[0]
        mysql_success_table_count = str(len(list_success_table))  # mysql创建成功的表总数
        table_failed_count = len(ddl_failed_table_result)  # mysql创建失败的表总数
        mysql_success_view_count = str(all_view_success_count)  # mysql视图创建成功的总数
        view_error_count = all_view_failed_count  # mysql创建视图失败的总数
        mysql_success_incol_count = str(all_inc_col_success_count)  # mysql自增列成功的总数
        autocol_error_count = all_inc_col_failed_count  # mysql自增列失败的总数
        mysql_success_constraint = str(all_constraints_success_count)  # mysql中索引以及约束创建成功的总数
        index_failed_count = str(constraint_failed_count)  # mysql中索引以及约束创建失败的总数
        mysql_success_fk = str(all_fk_success_count)  # mysql中外键创建成功的总数
        fk_failed_count = str(foreignkey_failed_count)  # mysql中外键创建失败的总数
        print('*' * 50 + 'MIGRATE SUMMARY' + '*' * 50 + '\n\n\n')
        print("Oracle MIGRATE TO MySQL FINISH\n" + "START TIME:" + str(mig_start_time) + '\n' + "FINISH TIME:" + str(
            mig_end_time) + '\n' + "ELAPSED TIME: " + str(
            (mig_end_time - mig_start_time).seconds) + "  seconds\n")
        print('\n\n\n')
        csv_file = open(log_path + "insert_table.csv", 'a', newline='')
        # 将MySQL创建成功的表总数记录保存到csv文件
        try:
            writer = csv.writer(csv_file)
            writer.writerow(('TOTAL:', mysql_success_table_count))
        except Exception as e:
            print(e)
        finally:
            csv_file.close()
        if ddl_failed_table_result:  # 输出失败的对象
            print("\n\nCREATE FAILED TABLE BELOW:")
            for output_ddl_failed_table_result in ddl_failed_table_result:
                print(output_ddl_failed_table_result)
            print('\n\n\n')
        if view_failed_result:
            print("CREATE FAILED VIEW BELOW: ")
            for output_fail_view in view_failed_result:
                print(output_fail_view)
            print('\n\n\n')
        print('TARGET DATABASE NAME: ' + mysql_database_name)
        # print('目标表成功创建计数: ' + str(mysql_table_count))
        print('1 TABLE  TOTAL: ' + str(
            oracle_tab_count) + ' TARGET SUCCESS TABLE: ' + mysql_success_table_count + ' TARGET FAILED TABLE: ' + str(
            table_failed_count))
        print('2 VIEW TOTAL: ' + str(
            oracle_view_count) + ' TARGET SUCCESS VIEW : ' + mysql_success_view_count + ' TARGET FAILED VIEW: ' + str(
            view_error_count))
        print('3 AUTO INCREMENT COL: ' + str(
            oracle_autocol_total) + ' TARGET SUCCESS COL: ' + mysql_success_incol_count + ' TARGET FAILED COL: ' + str(
            autocol_error_count))
        print('4 TRIGGER TOTAL: ' + str(
            normal_trigger_count) + ' TARGET SUCCESS TIGGER: ' + str(
            trigger_success_count) + ' TARGET FAILED TRIGGER: ' + str(
            trigger_failed_count))
        print('5 CONSTRAINT INDEX TOTAL: ' + str(
            oracle_constraint_count) + ' TARGET SUCCESS INDEX: ' + mysql_success_constraint + ' TARGET FAILED INDEX: ' + str(
            index_failed_count))
        print('6 FOREIGN KEY TOTAL: ' + str(
            oracle_fk_count) + ' TARGET SUCCESS FK: ' + mysql_success_fk + ' TARGET FAILED FK: ' +
              str(fk_failed_count))
        print('\nPLEASE CHECK FAILED TABLE DDL IN LOG DIR')
        print('Oracle PROCEDURE SAVED TO ' + exepath + '' + log_path + 'ddl_function_procedure.sql\n')
        print(
            'MIGATE LOG HAVE SAVED TO ' + exepath + '' + log_path + '\nPLEASE SELECT * FROM my_mig_task_info IN TARGET DATABASE\nINSERT FAILED TABLE PLEASE CHECK ddl_failed_table.log AND insert_failed_table.log\n\n')
        print('\n\n GENERATE REPORT ON CURRENT DIRECTORY "run_report.html"')
        table = HTMLTable(caption='RUN INFORMATION' + '------>DETAIL RUN LOG IN 👉' + exepath + '' + log_path + '👈')
        table.append_header_rows((('ID', 'OBJECT', 'TOTAL', 'SUCCESS', 'FAILED'),))
        # table[0][0].attr.colspan = 2
        table.append_data_rows((
            ('1', 'TABLE', str(oracle_tab_count), str(mysql_success_table_count), str(table_failed_count)),
            ('2', 'VIEW', str(oracle_view_count), str(mysql_success_view_count), str(view_error_count)),
            ('3', 'AUTO INCREMENT COL', str(oracle_autocol_total), str(mysql_success_incol_count),
             str(autocol_error_count)),
            ('4', 'TRIGGER', str(normal_trigger_count), str(trigger_success_count), str(trigger_failed_count)),
            ('5', 'CONSTRAINT INDEX', str(oracle_constraint_count), str(mysql_success_constraint),
             str(index_failed_count)),
            ('6', 'FOREIGN KEY', str(oracle_fk_count), str(mysql_success_fk), str(fk_failed_count)),
        ))
        table.caption.set_style({
            'font-size': '20px',
        })
        table.set_style({
            'border-collapse': 'collapse',
            'word-break': 'keep-all',
            'white-space': 'nowrap',
            'font-size': '14px',
        })
        table.set_cell_style({
            'border-color': '#000',
            'border-width': '1px',
            'border-style': 'solid',
            'padding': '5px',
        })
        table.set_header_row_style({
            'color': '#fff',
            'background-color': '#48a6fb',
            'font-size': '18px',
        })

        # 覆盖表头单元格字体样式
        table.set_header_cell_style({
            'padding': '15px',
        })
        table[1].set_cell_style({
            'padding': '8px',
            'font-size': '15px',
        })
        html = table.to_html()
        f = open("run_report.html", "w", encoding="utf-8")
        f.write(html)
        sql2 = "select convert(id,char) id,table_name,convert(source_table_rows,char) source_table_rows,convert(target_table_rows,char) target_table_rows,run_datail from (select (@i:= @i+1) as id,a.* from (select cc.table_name,cc.source_table_rows,cc.target_table_rows,case when cc.detail = '' then 'SUCCESS' else cc.detail end  run_datail from (select bb.table_name,bb.source_table_rows,bb.target_table_rows,concat(run_info,detail) detail from (select aa.table_name,aa.source_table_rows,aa.target_table_rows,case when aa.source_table_rows !=aa.target_table_rows then 'TABLE INSERT FAIL' else '' end run_info,aa.detail from (select table_name,max(source_table_rows) source_table_rows,sum(target_table_rows) target_table_rows,detail from my_mig_task_info where type='TABLE' group by table_name ) aa) bb) cc order by run_datail) a,(select @i:=0) b) ee"
        self.mysql_cursor.execute(sql2)
        sql_out = self.mysql_cursor.fetchall()
        table2 = HTMLTable(caption='TABLE REPORT')
        table2.append_header_rows((
            ('ID', 'TABLE_NAME', 'SOURCE_COUNT', 'TARGET_COUNT', 'RUN_DETAIL'),
        ))
        table2.append_data_rows(sql_out)
        table2.caption.set_style({
            'font-size': '20px',
        })
        table2.set_style({
            'border-collapse': 'collapse',
            'word-break': 'keep-all',
            'white-space': 'nowrap',
            'font-size': '14px',
        })
        table2.set_cell_style({
            'border-color': '#000',
            'border-width': '1px',
            'border-style': 'solid',
            'padding': '5px',
        })
        table2.set_header_row_style({
            'color': '#fff',
            'background-color': '#48a6fb',
            'font-size': '18px',
        })

        # 覆盖表头单元格字体样式
        table2.set_header_cell_style({
            'padding': '15px',
        })
        table2[1].set_cell_style({
            'padding': '8px',
            'font-size': '15px',
        })
        # 遍历数据行，如果增长量为负，标红背景颜色
        for row in table2.iter_data_rows():
            if row[4].value != 'SUCCESS':
                row.set_style({
                    'background-color': '#ffdddd',
                })
        html = table2.to_html()
        f = open("run_report.html", "a", encoding="utf-8")
        f.write(html)

    def cte_tab(self, log_path, is_custom_table):
        # db_meta = DbMetadata()
        output_table_name = []  # 用于存储要迁移的部分表
        list_success_table = []  # 创建成功的表
        ddl_failed_table_result = []  # 创建失败的表
        drop_table_name = output_table_name  # 创建表前先删除所有表
        if is_custom_table == 1:
            with open(log_path + "table.txt", "r") as f:  # 打开文件
                for line in f:
                    output_table_name.append(list(line.strip('\n').upper().split(',')))
        else:
            tableoutput_sql = """select table_name from user_tables  order by table_name  desc"""  # 查询需要导出的表
            output_table_name = self.oracle_cursor.fetch_all(tableoutput_sql)
            drop_table_name = self.oracle_cursor.fetch_all(tableoutput_sql)
        all_table_count = len(output_table_name)  # 无论是自定义表还是全库，都可以存入全局变量
        starttime = datetime.datetime.now()
        table_index = 0
        for drop_table in drop_table_name:
            drop_target_table = 'drop table if exists ' + drop_table[0]  # 现在是一次性把MySQL的表都删除
            self.mysql_cursor.execute(drop_target_table)
        for row in output_table_name:
            create_table_again = 0
            table_name = row[0]
            print('#' * 50 + ' CREATE TABLE ' + table_name + '#' * 50)
            #  将创建失败的sql记录到log文件
            logging.basicConfig(filename=log_path + 'ddl_failed_table.log')
            fieldinfos = []
            try:
                structs = self.tbl_columns(table_name)  # 获取源表的表字段信息
            except Exception as e:
                structs = []
                print('can not get column name,please check oracle table,maybe column is Virtual', e)
                filename = log_path + 'ddl_failed_table.log'
                f = open(filename, 'a', encoding='utf-8')
                f.write('-- ' + ' TABLE ' + table_name + ' ERROR ' + ' -- \n')
                f.write('\n' + '/*  can not get column name,please check oracle table,maybe column is Virtual ' + str(
                    e) + ' */' + '\n')
                f.close()
            # 以下字段已映射为MySQL字段类型
            for struct in structs:
                defaultvalue = struct.get('default')
                commentvalue = struct.get('comment')
                if defaultvalue:  # 对默认值以及注释数据类型的判断，如果不是str类型，转为str类型
                    defaultvalue = "'{0}'".format(defaultvalue) if type(defaultvalue) == 'str' else str(defaultvalue)
                if commentvalue:
                    commentvalue = "'{0}'".format(commentvalue) if type(commentvalue) == 'str' else str(commentvalue)
                    commentvalue = commentvalue.replace('"', '')  # 去掉注释中包含的双引号
                fieldinfos.append(
                    '{0} {1} {2} {3} {4}'.format('`' + struct['fieldname'] + '`',  # 2021-10-18增加了"`"MySQL的关键字
                                                 struct['type'],
                                                 # 'primary key' if struct.get('primary') else '',主键在创建表的时候定义
                                                 # ('default ' + '\'' + defaultvalue + '\'') if defaultvalue else '',
                                                 ('default ' + defaultvalue) if defaultvalue else '',  # 如果有默认值才加上default关键字
                                                 '' if struct.get('isnull') == 'True' else 'not null',
                                                 (
                                                         'comment ' + '"' + commentvalue + '"') if commentvalue else ''
                                                 ),

                )
            create_table_sql = 'create table {0} ({1})'.format(table_name, ','.join(fieldinfos))  # 生成创建目标表的sql
            format_sql = sql_format.sql_format(create_table_sql, wrap_add=[' ,', table_name + ' '], mode='upper')
            # add_pri_key_sql = 'alter table {0} add primary key ({1})'.format(table_name, ','.join(v_pri_key))  #
            # 创建目标表之后增加主键
            print((format_sql.replace('   ', ' ')).replace(' (`', ' (\n`'))
            try:
                # cur_createtbl.execute(create_table_sql)
                self.mysql_cursor.execute(create_table_sql)
                #  if v_pri_key: 因为已经有创建约束的sql，这里可以不用执行
                #    cur_createtbl.execute(add_pri_key_sql) 因为已经有创建约束的sql，这里可以不用执行
                print('SUCCESS CREATE', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '\n')
                filename = log_path + 'ddl_success_table.log'  # ddl创建成功的表，记录表名到ddl_success_table.log
                f = open(filename, 'a', encoding='utf-8')
                f.write(table_name + '\n')
                f.close()
                list_success_table.append(table_name)  # MySQL ddl创建成功的表也存到list中

            except Exception as e:
                # 上述遇到MySQL65535超出列总和长度，之后使用优化表结构tbl_columns(table_name,fix)方法再次尝试创建表
                print(str(e.args))
                if 'Row size too large' in str(e):
                    print('\n**Atention Begin Auto Decrease Varchar Column Size,Try For Create Table Again**\n')
                    fieldinfos = []
                    structs = self.tbl_columns(table_name, 'FIX')  # 使用fix方式获取源表实际列长度拼接列字段信息
                    for struct in structs:
                        defaultvalue = struct.get('default')
                        commentvalue = struct.get('comment')
                        if defaultvalue:  # 对默认值以及注释数据类型的判断，如果不是str类型，转为str类型
                            defaultvalue = "'{0}'".format(defaultvalue) if type(defaultvalue) == 'str' else str(
                                defaultvalue)
                        if commentvalue:
                            commentvalue = "'{0}'".format(commentvalue) if type(commentvalue) == 'str' else str(
                                commentvalue)
                            commentvalue = commentvalue.replace('"', '')  # 去掉注释中包含的双引号
                        fieldinfos.append(
                            '{0} {1} {2} {3} {4}'.format('`' + struct['fieldname'] + '`',  # 2021-10-18增加了"`"MySQL的关键字
                                                         struct['type'],
                                                         ('default ' + defaultvalue) if defaultvalue else '',
                                                         '' if struct.get('isnull') == 'True' else 'not null',
                                                         (
                                                                 'comment ' + '"' + commentvalue + '"') if commentvalue else ''
                                                         ),

                        )
                    create_table_sql = 'create table {0} ({1})'.format(table_name, ','.join(fieldinfos))  # 生成创建目标表的sql
                    create_table_sql = sql_format.sql_format(create_table_sql, wrap_add=None, mode='upper')
                    print(create_table_sql)
                    try:
                        self.mysql_cursor.execute(create_table_sql)
                        create_table_again = 1
                        print('SUCCESS CREATE', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '\n')
                        filename = log_path + 'ddl_success_table.log'  # ddl创建成功的表，记录表名到ddl_success_table.log
                        f = open(filename, 'a', encoding='utf-8')
                        f.write(table_name + '\n')
                        f.close()
                        list_success_table.append(table_name)  # MySQL ddl创建成功的表也存到list中
                        filename = log_path + 'optimize_table.sql'  # ddl创建成功的表，记录表名到ddl_success_table.log
                        f = open(filename, 'a', encoding='utf-8')
                        f.write(table_name + '\n')
                        f.close()
                    except Exception as ee:
                        table_index = table_index + 1
                        print('\n' + '/* ' + str(ee.args) + ' */' + '\n')
                        # print(traceback.format_exc())  # 如果某张表创建失败，遇到异常记录到log，会继续创建下张表
                        # ddl创建失败的表名记录到文件ddl_failed_table.log
                        filename = log_path + 'ddl_failed_table.log'
                        f = open(filename, 'a', encoding='utf-8')
                        f.write('-- ' + 'CREATE TABLE ' + table_name + ' ERROR ' + str(table_index) + ' -- \n')
                        f.write('/* ' + table_name + ' */' + '\n')
                        f.write(create_table_sql + ';\n')
                        f.write('\n' + '/* ' + str(ee.args) + ' */' + '\n')
                        f.close()
                        ddl_failed_table_result.append(table_name)  # 将当前ddl创建失败的表名记录到ddl_failed_table_result的list中
                        print('table ' + table_name + ' create failed\n')
                if create_table_again == 0:  # 忽略掉再次重建失败表的记录
                    ddl_failed_table_result.append(table_name)  # 将当前ddl创建失败的表名记录到ddl_failed_table_result的list中
                    table_index = table_index + 1
                    # ddl创建失败的表名记录到文件ddl_failed_table.log
                    filename = log_path + 'ddl_failed_table.log'
                    f = open(filename, 'a', encoding='utf-8')
                    f.write('-- ' + 'CREATE TABLE ' + table_name + ' ERROR ' + str(table_index) + ' -- \n')
                    f.write('/* ' + table_name + ' */' + '\n')
                    f.write(create_table_sql + ';\n')
                    f.write('\n' + '/* ' + str(e.args) + ' */' + '\n\n')
                    f.close()
                    print('table ' + table_name + ' create failed\n')
        endtime = datetime.datetime.now()
        print("CREATE TABLE RUN TIME\n" + "BEGIN TIME:" + str(starttime) + '\n' + "END TIME:" + str(
            endtime) + '\n' + "Elapsed:" + str(
            (endtime - starttime).seconds) + " seconds\n")
        print('#' * 50 + 'TABLE CREATE FINISH' + '#' * 50 + '\n\n\n')
        if len(ddl_failed_table_result) > 0:
            for fail_table_name in ddl_failed_table_result:
                try:
                    self.mysql_cursor.execute(
                        "insert into  my_mig_task_info(table_name,detail,type) values('%s','%s','%s')" % (
                            fail_table_name, 'TABLE NOT EXIST', 'TABLE'))
                except Exception as e:
                    print(e, 'insert table my_mig_task_info failed')
        return all_table_count, list_success_table, ddl_failed_table_result

    def cte_idx(self, log_path, is_custom_table):
        # 批量创建主键以及索引
        all_constraints_count = 0  # 约束以及索引总数（排除掉非normal index）
        all_constraints_success_count = 0  # mysql中约束以及索引创建成功的计数
        function_based_index_count = 0  # function_based_index总数
        function_based_index = []
        user_name = self.oracle_cursor.fetch_one("""select user from dual""")
        user_name = user_name[0]
        constraint_failed_count = 0
        output_table_name = []  # 迁移部分表
        create_index = ''
        all_index = []  # 存储执行创建约束的结果集
        start_time = datetime.datetime.now()
        print('#' * 50 + 'CREATE ' + 'CONSTRAINT AND INDEX  ' + '#' * 50)
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        # 以下是创建 NORMAL的主键以及普通索引
        if is_custom_table == 1:  # 如果命令行参数有-c选项，仅创建部分约束
            with open(log_path + "table.txt", "r") as f:  # 读取自定义表
                for line in f:
                    output_table_name.append(list(line.strip('\n').upper().split(',')))  # 将自定义表全部保存到list
            for v_custom_table in output_table_name:  # 读取第N个表查询生成拼接sql
                custom_index = self.oracle_cursor.fetch_all("""SELECT
                           (CASE
                             WHEN C.CONSTRAINT_TYPE = 'P' OR C.CONSTRAINT_TYPE = 'R' THEN
                              'ALTER TABLE ' || T.TABLE_NAME || ' ADD CONSTRAINT ' ||
                              '`'||T.INDEX_NAME||'`' || (CASE
                                WHEN C.CONSTRAINT_TYPE = 'P' THEN
                                 ' PRIMARY KEY ('
                                ELSE
                                 ' FOREIGN KEY ('
                              END) || listagg(T.COLUMN_NAME,',') within group(order by T.COLUMN_position) || ');'
                             ELSE
                              'CREATE ' || (CASE
                                WHEN I.UNIQUENESS = 'UNIQUE' THEN
                                 I.UNIQUENESS || ' '
                                ELSE
                                 CASE
                                   WHEN I.INDEX_TYPE = 'NORMAL' THEN
                                    ''
                                   ELSE
                                    I.INDEX_TYPE || ' '
                                 END
                              END) || 'INDEX ' || '`'||T.INDEX_NAME||'`' || ' ON ' || T.TABLE_NAME || '(' ||
                              listagg(T.COLUMN_NAME,',') within group(order by T.COLUMN_position) || ');'
                           END) SQL_CMD
                      FROM USER_IND_COLUMNS T, USER_INDEXES I, USER_CONSTRAINTS C
                     WHERE T.INDEX_NAME = I.INDEX_NAME
                       AND T.INDEX_NAME = C.CONSTRAINT_NAME(+)
                       AND T.TABLE_NAME = '%s'
                       and i.index_type != 'FUNCTION-BASED NORMAL'
                     GROUP BY T.TABLE_NAME,
                              T.INDEX_NAME,
                              I.UNIQUENESS,
                              I.INDEX_TYPE,
                              C.CONSTRAINT_TYPE""" % v_custom_table[0])
                for v_out in custom_index:  # 每次将上面单表全部结果集全部存到all_index的list里面
                    all_index.append(v_out)
        else:  # 命令行参数没有-c选项，创建所有约束
            all_index = self.oracle_cursor.fetch_all("""SELECT
                       (CASE
                         WHEN C.CONSTRAINT_TYPE = 'P' OR C.CONSTRAINT_TYPE = 'R' THEN
                          'ALTER TABLE ' || T.TABLE_NAME || ' ADD CONSTRAINT ' ||
                          '`'||T.INDEX_NAME||'`' || (CASE
                            WHEN C.CONSTRAINT_TYPE = 'P' THEN
                             ' PRIMARY KEY ('
                            ELSE
                             ' FOREIGN KEY ('
                          END) || listagg(T.COLUMN_NAME,',') within group(order by T.COLUMN_position) || ');'
                         ELSE
                          'CREATE ' || (CASE
                            WHEN I.UNIQUENESS = 'UNIQUE' THEN
                             I.UNIQUENESS || ' '
                            ELSE
                             CASE
                               WHEN I.INDEX_TYPE = 'NORMAL' THEN
                                ''
                               ELSE
                                I.INDEX_TYPE || ' '
                             END
                          END) || 'INDEX ' || '`'||T.INDEX_NAME||'`' || ' ON ' || T.TABLE_NAME || '(' ||
                          listagg(T.COLUMN_NAME,',') within group(order by T.COLUMN_position) || ');'
                       END) SQL_CMD
                  FROM USER_IND_COLUMNS T, USER_INDEXES I, USER_CONSTRAINTS C
                 WHERE T.INDEX_NAME = I.INDEX_NAME
                   AND T.INDEX_NAME = C.CONSTRAINT_NAME(+)
                   and i.index_type != 'FUNCTION-BASED NORMAL'
                 GROUP BY T.TABLE_NAME,
                          T.INDEX_NAME,
                          I.UNIQUENESS,
                          I.INDEX_TYPE,
                          C.CONSTRAINT_TYPE""")  # 如果要每张表查使用T.TABLE_NAME = '%s',%s传进去是没有单引号，所以需要用单引号号包围
        all_constraints_count = len(all_index)
        if all_constraints_count > 0:
            print('CREATE normal index:\n')
            index_num = 0
            for d in all_index:
                index_num += 1
                create_index_sql = d[0]  # 之前是wm_concat返回的是clob，所以用read读取大对象（d[0].read()），否则会报错
                print('-- ' + str(index_num) + ' ' + str(datetime.datetime.now()) + '\n' + create_index_sql + '\n')
                filename = log_path + 'create_index.sql'
                f = open(filename, 'a', encoding='utf-8')
                f.write('-- ' + str(index_num) + ' ' + str(datetime.datetime.now()) + '\n' + create_index_sql + '\n')
                f.close()
                try:
                    self.mysql_cursor.execute(
                        """insert into my_mig_task_info(table_name,task_start_time,run_status) values('%s',current_timestamp(3),'%s')""" % (
                            create_index_sql, 'running'))
                except Exception as e:
                    print(e)
                try:
                    self.mysql_cursor.execute(create_index_sql)
                    self.mysql_cursor.execute(
                        """update my_mig_task_info set run_status='end' where table_name='%s' """ % create_index_sql)
                    self.mysql_cursor.execute('commit')
                    all_constraints_success_count += 1
                except Exception as e:
                    constraint_failed_count += 1
                    print('\n' + '/* ' + str(e.args) + ' */' + '\n')
                    print('CREATE CONSTRAINT INDEX FAILED!\n')
                    filename = log_path + 'ddl_failed_table.log'
                    f = open(filename, 'a', encoding='utf-8')
                    f.write('\n-- ' + ' CONSTRAINTS CREATE ERROR ' + str(constraint_failed_count) + ' -- \n')
                    f.write(create_index_sql + '\n\n\n')
                    f.write('\n' + '/* ' + str(e.args) + ' */' + '\n')
                    f.close()
                    self.mysql_cursor.execute(
                        """update my_mig_task_info set run_status='failed' where table_name='%s' """ % create_index_sql)
                    self.mysql_cursor.execute('commit')
        else:
            print('NO normal index')
        # 以下是创建非normal索引
        if is_custom_table == 1:  # 如果命令行参数有-c选项，仅创建部分约束
            for v_custom_table in output_table_name:  # 读取第N个表
                function_index = self.oracle_cursor.fetch_all(
                    """Select index_name from user_indexes where index_type='FUNCTION-BASED NORMAL' and table_name ='%s'""" %
                    v_custom_table[0])  # 根据第N个表，获取所有所有名称
                for v_out0 in function_index:  # 将上述获取的若干索引名称一一存入list
                    function_based_index.append(v_out0)
        else:  # 查询所有表的索引名称
            function_based_index = self.oracle_cursor.fetch_all(
                """Select index_name from user_indexes where index_type='FUNCTION-BASED NORMAL'""")
        function_based_index_count = len(function_based_index)  # 如果有非normal索引
        if function_based_index_count > 0:
            print('CREATE NON normal index:\n')
        for v_function_based_index in function_based_index:
            fun_index_name = v_function_based_index[0]
            try:  # 下面是生成非normal索引的拼接sql，来源于dbms_metadata.get_ddl
                create_index = self.oracle_cursor.fetch_one(
                    """select trim(replace(regexp_replace(regexp_replace(SUBSTR(upper(to_char(dbms_metadata.get_ddl('INDEX','%s','%s'))), 1, INSTR(upper(to_char(dbms_metadata.get_ddl('INDEX','%s','%s'))), ' PCTFREE')-1),'"','',1,0,'i'),'%s'||'.','',1,0,'i'),chr(10),'')) from dual""" % (
                        fun_index_name, user_name, fun_index_name, user_name, user_name))
                create_index = create_index[0]
                print(create_index)
                self.mysql_cursor.execute(create_index)
                print('success\n')
                all_constraints_success_count += 1
            except Exception as e:
                constraint_failed_count += 1
                print('\n' + '/* ' + str(e.args) + ' */' + '\n')
                print('NON NORMAL INDEX CREATE ERROR\n')
                filename = log_path + 'ddl_failed_table.log'
                f = open(filename, 'a', encoding='utf-8')
                f.write('\n-- ' + 'NON NORMAL INDEX CREATE ERROR ' + str(constraint_failed_count) + '\n')
                f.write(create_index + ';\n')
                f.write('\n' + '/* ' + str(e.args) + ' */' + '\n')
                f.close()
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        end_time = datetime.datetime.now()
        print('CREATE INDEX CONSTRAINT ELAPSED TIME: ' + str((end_time - start_time).seconds))
        print('#' * 50 + 'CONSTRAINT INDEX FINISH' + '#' * 50 + '\n\n\n')
        return all_constraints_count, all_constraints_success_count, function_based_index_count, constraint_failed_count

    def fk(self, log_path, is_custom_table):
        # 批量创建外键
        """
        11g以及之前的能用WMSYS.WM_CONCAT(A.COLUMN_NAME)，之后需使用listagg(A.COLUMN_NAME,',') within group(order by a.position)
        """
        all_fk_count = 0
        all_fk_success_count = 0
        fk_err_count = 0
        begin_time = datetime.datetime.now()
        fk_table = []  # 存储要创建外键的表
        print('#' * 50 + 'CREATE ' + 'FOREIGN KEY ' + '#' * 50)
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        if is_custom_table == 1:  # 如果命令行参数有-c选项，仅创建部分外键
            with open(log_path + "table.txt", "r") as f:
                for line in f:  # 将自定义表存到list
                    fk_table.append(list(line.strip('\n').upper().split(',')))
        else:  # 创建全部外键
            table_foreign_key = 'select table_name from USER_CONSTRAINTS where CONSTRAINT_TYPE= \'R\''
            try:
                fk_table = self.oracle_cursor.fetch_all(table_foreign_key)
            except Exception as e:
                print(e)
                fk_table = []
        if len(fk_table) > 0:
            print('START CREATE FOREIGN KEY')
            for v_result_table in fk_table:  # 获得一张表创建外键的拼接语句，按照每张表顺序来创建外键
                table_name = v_result_table[0]
                try:
                    all_foreign_key = self.oracle_cursor.fetch_all("""SELECT 'ALTER TABLE ' || B.TABLE_NAME || ' ADD CONSTRAINT ' ||
                                B.CONSTRAINT_NAME || ' FOREIGN KEY (' ||
                                (SELECT listagg(A.COLUMN_NAME,',') within group(order by a.position)
                                   FROM USER_CONS_COLUMNS A
                                  WHERE A.CONSTRAINT_NAME = B.CONSTRAINT_NAME) || ') REFERENCES ' ||
                                (SELECT B1.table_name FROM USER_CONSTRAINTS B1
                                  WHERE B1.CONSTRAINT_NAME = B.R_CONSTRAINT_NAME) || '(' ||
                                (SELECT listagg(A.COLUMN_NAME,',') within group(order by a.position)
                                   FROM USER_CONS_COLUMNS A
                                  WHERE A.CONSTRAINT_NAME = B.R_CONSTRAINT_NAME) || ');'
                           FROM USER_CONSTRAINTS B
                          WHERE B.CONSTRAINT_TYPE = 'R' and TABLE_NAME='%s'""" % table_name)
                except Exception as e:
                    all_foreign_key = []
                    fk_err_count += 1
                    print('table ', table_name, 'create foreign key failed ', e)
                for e in all_foreign_key:  # 根据上面的查询结果集，创建外键
                    create_foreign_key_sql = e[0]
                    print(create_foreign_key_sql)
                    all_fk_count += 1  # 外键总数
                    try:
                        self.mysql_cursor.execute(
                            """insert into my_mig_task_info(table_name,task_start_time,run_status) values('%s',current_timestamp(3),'%s')""" % (
                                create_foreign_key_sql, 'running'))
                    except Exception as e:
                        print(e)
                    try:
                        self.mysql_cursor.execute(create_foreign_key_sql)
                        print('FINISH CREATE FOREIGN KEY\n')
                        all_fk_success_count += 1
                        self.mysql_cursor.execute(
                            """update my_mig_task_info set run_status='end' where table_name='%s' """ % create_foreign_key_sql)
                        self.mysql_cursor.execute('commit')
                    except Exception as e:
                        fk_err_count += 1
                        print('\n' + '/* ' + str(e.args) + ' */' + '\n')
                        print('CREATE FOREIGN KEY ERROR PLEASE CHECK DDL!\n')
                        # print(traceback.format_exc())
                        filename = log_path + 'ddl_failed_table.log'
                        f = open(filename, 'a', encoding='utf-8')
                        f.write('\n-- ' + ' FOREIGNKEY CREATE ERROR ' + str(fk_err_count) + '\n')
                        f.write(create_foreign_key_sql + ';\n')
                        f.write('\n' + '/* ' + str(e.args) + ' */' + '\n')
                        f.close()
                        self.mysql_cursor.execute(
                            """update my_mig_task_info set run_status='failed' where table_name='%s' """ % create_foreign_key_sql)
                        self.mysql_cursor.execute('commit')
        else:
            print('NO FOREIGN KEY')
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        end_time = datetime.datetime.now()
        print('CREATE FOREIGN KEY ELASPSED TIME: ' + str((end_time - begin_time).seconds))
        print('#' * 50 + 'FINISH CREATE FOREIGN KEY' + '#' * 50 + '\n\n\n')
        return all_fk_count, all_fk_success_count, fk_err_count

    def cte_trg(self, log_path, is_custom_table):
        # 查找具有自增特性的表以及字段名称
        all_inc_col_success_count = 0
        all_inc_col_failed_count = 0
        oracle_autocol_total = 0
        normal_trigger_count = 0  # 用于统计oracle触发器（排除掉序列相关触发器）的总数
        trigger_success_count = 0  # mysql中触发器创建成功的总数
        trigger_failed_count = 0  # mysql中触发器创建失败的总数
        normal_trigger = []  # 用于存自定义表读取之后的触发器名称
        create_trigger = ''  # 触发器创建的sql
        count_1 = 0  # 自增列索引创建失败的计数
        start_time = datetime.datetime.now()
        user_name = self.oracle_cursor.fetch_one("""select user from dual""")
        user_name = user_name[0]
        # Oracle中无法对long类型数据截取，创建用于存储触发器字段信息的临时表TRIGGER_NAME
        try:
            count_num_tri = \
                self.oracle_cursor.fetch_one("""select count(*) from user_tables where table_name='TRIGGER_NAME'""")[0]
        except Exception as e:
            count_num_tri = 0
            print(e)
        if count_num_tri == 1:  # 判断表trigger_name是否存在
            try:
                self.oracle_cursor.execute_sql("""truncate table trigger_name""")
            except Exception:
                print(traceback.format_exc())
                print('truncate table trigger_name失败')
        else:
            try:
                self.oracle_cursor.execute_sql(
                    """create table trigger_name (table_name varchar2(200),trigger_type varchar2(100),trigger_body clob)""")
            except Exception:
                print(traceback.format_exc())
                print('CREATE trigger_name IN ORACLE FAILED')
        print('#' * 50 + 'BEGIN CREATE AUTO COL' + '#' * 50)
        if is_custom_table == 1:  # 如果命令行参数有-c选项，仅创建部分自增列
            with open(log_path + "table.txt", "r") as f:  # 读取自定义表
                for table_name in f.readlines():  # 按顺序读取每一个表
                    table_name = table_name.strip('\n').upper()  # 去掉列表中每一个元素的换行符
                    # Oracle中无法对long类型数据截取，创建用于存储触发器字段信息的临时表TRIGGER_NAME
                    try:  # 按照每张表，将单张表结果集插入到trigger_name
                        self.oracle_cursor.execute_sql(
                            """insert into trigger_name select table_name ,trigger_type,to_lob(trigger_body) from user_triggers where table_name= '%s'  """ % table_name)
                    except Exception:
                        print(traceback.format_exc())
                        print('insert into trigger_name ERROR')
                    try:
                        self.oracle_cursor.execute_sql("""update trigger_name set trigger_body=upper(trigger_body) """)
                        self.oracle_cursor.execute_sql(
                            """update trigger_name set trigger_body=replace(trigger_body,'INTO:','INTO :')""")
                        self.oracle_cursor.execute_sql(
                            """update trigger_name set trigger_body=replace(trigger_body,'SYS.DUAL ','DUAL')""")
                        self.oracle_cursor.execute_sql(
                            """update trigger_name set trigger_body=replace(trigger_body,'SYS.DUAL','DUAL')""")
                        self.oracle_cursor.execute_sql(
                            """update trigger_name set trigger_body=replace(trigger_body,chr(10),'')""")
                    except Exception:
                        print(traceback.format_exc())
                        print('update trigger_name ERROR')

        else:  # 创建所有自增列索引
            try:
                self.oracle_cursor.execute_sql("""truncate table trigger_name""")
                self.oracle_cursor.execute_sql(
                    """insert into trigger_name select table_name ,trigger_type,to_lob(trigger_body) from user_triggers""")
            except Exception:
                print(traceback.format_exc())
                print('insert into trigger_name ERROR')
            try:
                self.oracle_cursor.execute_sql("""update trigger_name set trigger_body=upper(trigger_body) """)
                self.oracle_cursor.execute_sql(
                    """update trigger_name set trigger_body=replace(trigger_body,'INTO:','INTO :')""")
                self.oracle_cursor.execute_sql(
                    """update trigger_name set trigger_body=replace(trigger_body,'SYS.DUAL ','DUAL')""")
                self.oracle_cursor.execute_sql(
                    """update trigger_name set trigger_body=replace(trigger_body,'SYS.DUAL','DUAL')""")
                self.oracle_cursor.execute_sql(
                    """update trigger_name set trigger_body=replace(trigger_body,chr(10),'')""")
            except Exception:
                print(traceback.format_exc())
                print('update trigger_name ERROR')
        try:
            all_create_index = self.oracle_cursor.fetch_all(
                """select distinct sql_create from (select to_char('create index ids_'||substr(table_name,1,26)||' on '||table_name||'('||upper(substr(substr(SUBSTR(trigger_body, INSTR(upper(trigger_body), ':NEW.') + 1,length(trigger_body) - instr(trigger_body, ':NEW.')), 1, instr(upper(SUBSTR(trigger_body, INSTR(upper(trigger_body), ':NEW.') + 1,length(trigger_body) - instr(trigger_body, ':NEW.'))), ' FROM DUAL;') - 1), 5)) ||');') as sql_create from trigger_name where trigger_type='BEFORE EACH ROW' and instr(upper(trigger_body), 'NEXTVAL')>0 AND TRIGGER_BODY LIKE '%INTO :%' )""")  # 在Oracle拼接sql生成用于在MySQL中自增列的索引
        except Exception as e:
            all_create_index = []
            print(e)
        auto_inc_count = len(all_create_index)
        if auto_inc_count > 0:
            print('CREATE INDEX FOR AUTO COL:\n ')
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
            index_num = 0
            for v_increa_index in all_create_index:
                index_num += 1
                create_autoincrea_index = v_increa_index[0]  # v_increa_index[0].read()  # 大字段用read读取大字段，否则无法执行
                print(
                    '-- ' + str(index_num) + ' ' + str(datetime.datetime.now()) + '\n' + create_autoincrea_index + '\n')
                filename = log_path + 'create_index.sql'
                f = open(filename, 'a', encoding='utf-8')
                f.write(
                    '-- ' + str(index_num) + ' ' + str(datetime.datetime.now()) + '\n' + create_autoincrea_index + '\n')
                f.close()
                try:
                    self.mysql_cursor.execute(
                        """insert into my_mig_task_info(table_name,task_start_time,run_status) values('%s',current_timestamp(3),'%s')""" % (
                            create_autoincrea_index, 'running'))
                except Exception as e:
                    print(e)
                try:
                    self.mysql_cursor.execute(create_autoincrea_index)
                    self.mysql_cursor.execute(
                        """update my_mig_task_info set run_status='end' where table_name='%s' """ % create_autoincrea_index)
                    self.mysql_cursor.execute('commit')
                except Exception as e:
                    count_1 += 1
                    all_inc_col_failed_count += 1
                    print('\n' + '/* ' + str(e) + ' */' + '\n')
                    print('create_autoincrea_index ERROR\n')
                    # print(traceback.format_exc())
                    filename = log_path + 'ddl_failed_table.log'
                    f = open(filename, 'a', encoding='utf-8')
                    f.write('-- ' + str(count_1) + ' AUTO_INCREAMENT COL INDEX CREATE ERROR' + ' -- ' + '\n')
                    f.write(create_autoincrea_index + '\n\n\n')
                    f.close()
                    self.mysql_cursor.execute(
                        """update my_mig_task_info set run_status='failed' where table_name='%s' """ % create_autoincrea_index)
                    self.mysql_cursor.execute('commit')
                    ddl_incindex_error = '\n' + '/* ' + str(e.args) + ' */' + '\n'
                    logging.error(ddl_incindex_error)  # 自增用索引创建失败的sql语句输出到文件ddl_failed_table.log
            print('AUTO COL INDEX FINISH ' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

            print('\nSTART MODIFY AUTO COL ATTRIBUTE:')
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
            try:
                all_alter_sql = self.oracle_cursor.fetch_all(
                    """SELECT to_char('alter table ' || table_name || ' modify ' || upper( substr( substr( SUBSTR( trigger_body, INSTR( upper( trigger_body ), ':NEW.' ) + 1, length( trigger_body ) - instr( trigger_body, ':NEW.' )),1, instr( upper( SUBSTR( trigger_body, INSTR( upper( trigger_body ), ':NEW.' ) + 1, length( trigger_body ) - instr( trigger_body, ':NEW.' ))), ' FROM DUAL;' ) - 1  ),  5  )) || ' bigint auto_increment;' ) FROM trigger_name  WHERE trigger_type = 'BEFORE EACH ROW' AND TRIGGER_BODY LIKE '%INTO :%'  AND instr( upper( trigger_body ), 'NEXTVAL' )> 0""")
            except Exception as e:
                print(e)
                all_alter_sql = []
            auto_num = 0
            for v_increa_col in all_alter_sql:
                auto_num += 1
                alter_increa_col = v_increa_col[0]  # v_increa_col[0].read()  # 大字段用read读取大字段，否则无法执行
                print('-- ' + str(auto_num) + ' ' + str(datetime.datetime.now()) + '\n' + alter_increa_col + '\n')
                filename = log_path + 'create_index.sql'
                f = open(filename, 'a', encoding='utf-8')
                f.write('-- ' + str(auto_num) + ' ' + str(datetime.datetime.now()) + '\n' + alter_increa_col + '\n')
                f.close()
                try:
                    self.mysql_cursor.execute(
                        """insert into my_mig_task_info(table_name,task_start_time,run_status) values('%s',current_timestamp(3),'%s')""" % (
                            alter_increa_col, 'running'))
                except Exception as e:
                    print(e)
                try:  # 注意下try要在for里面
                    self.mysql_cursor.execute(alter_increa_col)
                    self.mysql_cursor.execute(
                        """update my_mig_task_info set run_status='end' where table_name='%s' """ % alter_increa_col)
                    self.mysql_cursor.execute('commit')
                    all_inc_col_success_count += 1
                except Exception as e:  # 如果有异常打印异常信息，并跳过继续下个自增列修改
                    all_inc_col_failed_count += 1
                    print('\n' + '/* ' + str(e) + ' */' + '\n')
                    print('ALTER AUTO COL FAIL\n')
                    # print(traceback.format_exc())
                    filename = log_path + 'ddl_failed_table.log'
                    f = open(filename, 'a', encoding='utf-8')
                    f.write('\n-- ' + ' MODIFY AUTO_COL ERROR ' + str(all_inc_col_failed_count) + ' -- \n')
                    f.write(alter_increa_col + ';\n')
                    f.write('\n' + '/* ' + str(e) + ' */' + '\n')
                    f.close()
                    self.mysql_cursor.execute(
                        """update my_mig_task_info set run_status='failed' where table_name='%s' """ % alter_increa_col)
                    self.mysql_cursor.execute('commit')
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
            end_time = datetime.datetime.now()
            print('ALTER AUTO COL ELAPSED TIME: ' + str((end_time - start_time).seconds))
            print('#' * 50 + 'FINISH AUTO COL' + '#' * 50 + '\n\n\n')
            try:
                oracle_autocol_total = self.oracle_cursor.fetch_one(
                    """select count(*) from trigger_name  where trigger_type='BEFORE EACH ROW' and instr(upper(trigger_body), 'NEXTVAL')>0""")[
                    0]  # 将自增列的总数存入list
            except Exception as e:
                print(e)
                oracle_autocol_total = 0
        else:
            print('NO AUTO COL')
        print('#' * 50 + 'END AUTO COL' + '#' * 50 + '\n')
        try:
            self.oracle_cursor.execute_sql("""drop table trigger_name purge""")  # 删除之前在oracle创建的临时表
        except Exception as e:
            print(e)

        # 以下是创建常规触发器
        if is_custom_table == 1:  # 如果命令行参数有-c选项，仅创建部分自增列
            with open(log_path + "table.txt", "r") as f:  # 读取自定义表
                for table_name in f.readlines():  # 按顺序读取每一个表
                    table_name = table_name.strip('\n').upper()  # 去掉列表中每一个元素的换行符
                    try:
                        trigger_name = self.oracle_cursor.fetch_all(
                            """select trigger_name from user_triggers where trigger_type !='BEFORE EACH ROW' and table_name='%s'""" % table_name)
                        for v_trt_name in trigger_name:
                            normal_trigger.append(v_trt_name)
                    except Exception as e:
                        print(e)
        else:
            try:
                normal_trigger = self.oracle_cursor.fetch_all(
                    """select trigger_name from user_triggers where trigger_type !='BEFORE EACH ROW'""")
            except Exception as e:
                print(e)
        normal_trigger_count = len(normal_trigger)
        if normal_trigger_count > 0:
            print('START CREATE NORMAL TRIGGER:\n')
            for v_normal_trigger in normal_trigger:
                trigger_name = v_normal_trigger[0]
                try:
                    create_trigger = self.oracle_cursor.fetch_one(
                        """select regexp_replace(regexp_replace(trim(replace(regexp_replace(regexp_replace(SUBSTR(upper(dbms_metadata.get_ddl('TRIGGER','%s','%s')), 1, INSTR(upper(dbms_metadata.get_ddl('TRIGGER','%s','%s')), 'ALTER TRIGGER')-1),'"','',1,0,'i'),'%s.','',1,0,'i'),chr(10),'')),'OR REPLACE','',1,0,'i'),':','',1,0,'i') from dual""" % (
                            trigger_name, user_name, trigger_name, user_name, user_name))
                    create_trigger = create_trigger[0].read()
                    print(create_trigger)
                    self.mysql_cursor.execute(create_trigger)
                    trigger_success_count += 1
                    print('CREATE TRIGGER FINISH\n')
                except Exception as e:
                    trigger_failed_count += 1
                    print('\n' + '/* ' + str(e) + ' */' + '\n')
                    print('CREATE TRIGGER ERROR!\n')
                    filename = log_path + 'ddl_failed_table.log'
                    f = open(filename, 'a', encoding='utf-8')
                    f.write('\n-- ' + ' NORMAL TRIGGER CREATE ERROR' + str(trigger_failed_count) + ' -- \n')
                    f.write(create_trigger + ';\n')
                    f.write('\n' + '/* ' + str(e.args) + ' */' + '\n')
                    f.close()
        else:
            print('NO TRIGGER\n')
        return all_inc_col_success_count, all_inc_col_failed_count, normal_trigger_count, trigger_success_count, \
               trigger_failed_count, oracle_autocol_total

    def cte_comt(self, log_path, is_custom_table):
        # 数据库对象的comment注释,这里仅包含表的注释，列的注释在上面创建表结构的时候已经包括
        err_count = 0
        all_comment_sql = []
        output_table_name = []
        begin_time = datetime.datetime.now()
        print('#' * 50 + 'START CREATE comment' + '#' * 50)
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        if is_custom_table == 1:  # 命令行选项-c指定后，仅创建部分注释
            with open(log_path + "table.txt", "r") as f:  # 读取自定义表
                for line in f:
                    output_table_name.append(list(line.strip('\n').upper().split(',')))  # 读取txt中的自定义表到list
            for v_custom_table in output_table_name:  # 根据第N个表查询生成拼接sql
                try:
                    custom_comment = self.oracle_cursor.fetch_all(
                        """select 'alter table '||TABLE_NAME||' comment '||''''||COMMENTS||'''' as create_comment from USER_TAB_COMMENTS where COMMENTS is not null and table_name = '%s'  """ %
                        v_custom_table[0])
                except Exception as e:
                    print(e)
                    custom_comment = []
                for v_out in custom_comment:  # 每次将上面单表全部结果集全部存到all_comment_sql的list里面
                    all_comment_sql.append(v_out)
        else:  # 创建全部注释
            try:
                all_comment_sql = self.oracle_cursor.fetch_all(
                    """select 'alter table '||TABLE_NAME||' comment '||''''||COMMENTS||'''' as create_comment  from USER_TAB_COMMENTS where COMMENTS is not null """)
            except Exception as e:
                print(e)
                all_comment_sql = []
        if len(all_comment_sql) > 0:
            for e in all_comment_sql:  # 一次性创建注释
                # table_name = e[0]
                create_comment_sql = e[0]
                try:
                    print('MODIFING COMMENT:')
                    print(create_comment_sql)
                    # cur_target_constraint.execute(create_comment_sql)
                    self.mysql_cursor.execute(create_comment_sql)
                    print('comment FINISH\n')
                except Exception as e:
                    err_count += 1
                    print('\n' + '/* ' + str(e.args) + ' */' + '\n')
                    print('comment FAILED!\n')
                    # print(traceback.format_exc())
                    filename = log_path + 'ddl_failed_table.log'
                    f = open(filename, 'a', encoding='utf-8')
                    f.write('\n-- ' + ' CREATE COMMENT ERROR ' + str(err_count) + '\n')
                    f.write(create_comment_sql + ';\n')
                    f.write('\n' + '/* ' + str(e.args) + ' */' + '\n')
                    f.close()
        else:
            print('NO COMMENT')
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        end_time = datetime.datetime.now()
        print('CREATE  COMMENT ELAPSED TIME:' + str((end_time - begin_time).seconds))
        print('#' * 50 + 'comment FINISH' + '#' * 50 + '\n\n\n')

    def cp_vw(self):
        # 重新编译Oracle视图
        try:
            all_view_compile = self.oracle_cursor.fetch_all(
                """select 'alter view '||view_name||' compile' from user_views""")
        except Exception as e:
            print(e)
            all_view_compile = []
        for exe_compile in all_view_compile:
            exe_compile_sql = exe_compile[0]
            print(exe_compile_sql)
            try:
                self.oracle_cursor.execute_sql(exe_compile_sql)
            except Exception as e:
                print(e)

    def c_vw(self, log_path, is_custom_table):
        # 获取视图定义以及创建
        all_view_count = 0
        view_failed_result = []
        all_view_success_count = 0
        all_view_failed_count = 0
        begin_time = datetime.datetime.now()
        if is_custom_table == 1:  # 如果命令行-c开启就不创建视图
            print('\n\n' + '#' * 50 + 'NO VIEW CREATE' + '#' * 50 + '\n')
        else:  # 创建全部视图
            print('#' * 50 + 'START CREATE VIEW' + '#' * 50)
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
            # Oracle中无法对long类型数据截取，创建用于存储视图信息的临时表content_view
            try:
                count_num_view = \
                    self.oracle_cursor.fetch_one(
                        """select count(*) from user_tables where table_name='CONTENT_VIEW'""")[0]
            except Exception as e:
                print(e)
                count_num_view = 0
            if count_num_view == 1:
                try:
                    self.oracle_cursor.execute_sql("""drop table CONTENT_VIEW purge""")
                    self.oracle_cursor.execute_sql("""create table content_view (view_name varchar2(200),text clob)""")
                    self.oracle_cursor.execute_sql(
                        """insert into content_view(view_name,text) select view_name,to_lob(text) from USER_VIEWS where  view_name in (select object_name from user_objects where object_type='VIEW' and status='VALID')""")
                except Exception as e:
                    print(e)
            else:
                try:
                    self.oracle_cursor.execute_sql("""create table content_view (view_name varchar2(200),text clob)""")
                    self.oracle_cursor.execute_sql(
                        """insert into content_view(view_name,text) select view_name,to_lob(text) from USER_VIEWS where  view_name in (select object_name from user_objects where object_type='VIEW' and status='VALID')""")
                except Exception as e:
                    print(e)
            try:
                all_view_create = self.oracle_cursor.fetch_all(
                    """select  view_name,'create view '||view_name||' as '||replace(text, '"'  , '') as view_sql from CONTENT_VIEW """)
            except Exception as e:
                print(e)
                all_view_create = []
            all_view_count = len(all_view_create)
            if all_view_count > 0:
                view_count = 0
                for e in all_view_create:
                    view_name = e[0]
                    create_view_sql = e[1].read()  # 用read读取大字段，否则无法执行
                    create_view_sql = sql_format.sql_format(create_view_sql, wrap_add=None, mode='upper')  # sql格式化
                    create_view_sql = create_view_sql.replace('--', '-- -- ')  # 注释适配为MySQL
                    create_view_sql = create_view_sql.replace('nvl(', 'ifnull(')  # 函数替换
                    create_view_sql = create_view_sql.replace('unistr(\'\\0030\')', '0')  # 函数替换
                    create_view_sql = create_view_sql.replace('unistr(\'\\0031\')', '1')  # 函数替换
                    create_view_sql = create_view_sql.replace('unistr(\'\\0033\')', '3')  # 函数替换
                    print(create_view_sql)
                    view_count += 1
                    filename = log_path + 'create_view.sql'
                    f = open(filename, 'a', encoding='utf-8')
                    f.write('\n-- ' + str(view_count) + ' ' + view_name + ' -- \n')
                    f.write(create_view_sql + ';\n')
                    f.write('\n')
                    f.close()
                    try:
                        # cur_target_constraint.execute("""drop view  if exists %s""" % view_name)
                        # cur_target_constraint.execute(create_view_sql)
                        self.mysql_cursor.execute("""drop view  if exists %s""" % view_name)
                        self.mysql_cursor.execute(create_view_sql)
                        print('FINISH CREATE VIEW\n')
                        all_view_success_count += 1
                    except Exception as e:
                        all_view_failed_count += 1
                        view_failed_result.append(view_name)
                        print('\n' + '/* ' + str(e.args) + ' */' + '\n')
                        print('CREATE VIEW ERROR!\n')
                        # print(traceback.format_exc())
                        filename = log_path + 'ddl_failed_table.log'
                        f = open(filename, 'a', encoding='utf-8')
                        f.write(
                            '\n-- ' + ' CREATE VIEW ' + view_name + ' ERROR ' + str(all_view_failed_count) + ' -- \n')
                        f.write(create_view_sql + ';\n')
                        f.write('\n' + '/* ' + str(e.args) + ' */' + '\n')
                        f.close()
            else:
                print('NO VIEW CREATE')
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
            end_time = datetime.datetime.now()
            print('CREATE VIEW ELAPSED TIME: ' + str((end_time - begin_time).seconds))
            print('*' * 50 + 'FINISH CREATE VIEW' + '*' * 50 + '\n\n\n')
            self.oracle_cursor.execute_sql("""drop table content_view purge""")
            return all_view_count, all_view_success_count, all_view_failed_count, view_failed_result

    def func_proc(self, log_path):
        # 输出函数以及存储过程定义
        index = 0
        filename = log_path + 'ddl_function_procedure.sql'
        f = open(filename, 'a', encoding='utf-8')
        f.write('/*EXPORT FROM ORACLE DATABASE FUNCTION AND PROCEDURE ' + '*/\n')
        f.close()
        try:
            ddl_out = self.oracle_cursor.fetch_all(
                """SELECT DBMS_METADATA.GET_DDL(U.OBJECT_TYPE, u.object_name) ddl_sql,u.object_name,u.object_type,u.status,(select user from dual) FROM USER_OBJECTS u where U.OBJECT_TYPE IN ('FUNCTION','PROCEDURE','PACKAGE') order by OBJECT_TYPE""")
            for v_out_ddl in ddl_out:
                index += 1
                ddl_sql = str(v_out_ddl[0])
                object_name = v_out_ddl[1]
                object_type = v_out_ddl[2]
                status = v_out_ddl[3]
                current_user = v_out_ddl[4]
                f = open(filename, 'a', encoding='utf-8')
                f.write('\n/*' + '[' + str(
                    index) + '] ' + object_type + ' ' + object_name.upper() + ' [' + status + ']' + '*/\n')
                f.write((ddl_sql.replace('"' + current_user + '".', '')).replace('"', ''))  # 去掉模式名以及双引号包围
                f.close()
        except Exception as e:
            print('get function and procedure content failed' + str(e))
