import configparser
import os
import sys

# 读取配置文件
# 以下在pycharm，主程序调用正常，本页文件执行失败，pyinstall生成的可执行文件可读取config文件
exepath = os.path.dirname(os.path.abspath(__file__))
config = configparser.ConfigParser()
config.read('config.ini', encoding='UTF-8')

# 以下在pycharm环境以及linux本机运行正常，pyinstall生成的可执行文件无法读取config文件
'''
config = configparser.ConfigParser()  # 调用配置文件读取
exepath = os.path.dirname(sys.path[0])  # 这里获取的是当前可执行文件或者主程序入口文件目录.的上一层路径，所以需要根据这个调整读取配置文件的路径
config.read(exepath + "/oracle_to_mysql/config.ini", encoding='utf-8')
'''


class ReadConfig:
    def get_mysql(self, name):
        value = config.get('mysql', name)  # 通过config.get拿到配置文件中DATABASE的name的对应值
        return value

    def get_oracle(self, name):
        value = config.get('oracle', name)  # 通过config.get拿到配置文件中DATABASE的name的对应值
        return value


if __name__ == '__main__':
    print('path值为：', exepath)  # 测试path内容
    print('config_path', exepath + "/db_config/config.ini")  # 打印输出config_path测试内容是否正确
    print('通过config.get拿到配置文件中DATABASE的host的对应值:',
          ReadConfig().get_mysql('host'))  # 通过上面的ReadConfig().get_mysql方法获取配置文件中DATABASE的'host'的对应值为10.182.27.158
