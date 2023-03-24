rm -rf package/*.py
cp db_info.py package
cp configDB.py package
cp readConfig.py package
cp sql_format.py package
rm -rf /opt/pycode/oracle_to_mysql/oracle_to_mysql/*
pyinstaller -F --clean -p package oracle_mig_mysql.py --distpath oracle_to_mysql
pyinstaller -F --clean -p package oracle_compare_mysql.py --distpath oracle_to_mysql
cp config.ini custom_table.txt env_ora.sh oracle_to_mysql
cp -apr oracle_client oracle_to_mysql
rm -rf oracle_mig_mysql.zip
zip -r oracle_mig_mysql_linux_x86.zip oracle_to_mysql
scp /opt/pycode/oracle_to_mysql/oracle_to_mysql/oracle_mig_mysql    root@192.168.189.200:/opt/oracle_to_mysql
scp /opt/pycode/oracle_to_mysql/oracle_to_mysql/oracle_compare_mysql root@192.168.189.200:/opt/oracle_to_mysql
scp /opt/pycode/oracle_to_mysql/oracle_to_mysql/config.ini root@192.168.189.200:/opt/oracle_to_mysql
scp /opt/pycode/oracle_to_mysql/oracle_to_mysql/env_ora.sh root@192.168.189.200:/opt/oracle_to_mysql