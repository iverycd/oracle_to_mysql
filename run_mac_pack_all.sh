#!/bin/bash
# pack linux release zip file
ssh -p 22 root@192.168.189.208 "cd /opt/pycode/oracle_to_mysql && ls && sh pack.sh"
# cp linux release to MacOS 192.168.149.79
scp -P 22 root@192.168.189.208:/opt/pycode/oracle_to_mysql/oracle_mig_mysql_linux_x86.zip /Users/kay/Documents/release
# pack MacOS release zip file
cd /Users/kay/Documents/python_code/oracle_to_mysql
rm -rf package/*.py
cp db_info.py package
cp configDB.py package
cp readConfig.py package
cp sql_format.py package
/Users/kay/opt/anaconda3/bin/pyinstaller -F --clean -p package oracle_mig_mysql.py
/Users/kay/opt/anaconda3/bin/pyinstaller -F --clean -p package oracle_compare_mysql.py
rm -rf dist/oracle_to_mysql
mkdir -p dist/oracle_to_mysql
cp -r oracle_client dist/oracle_to_mysql
cp config.ini custom_table.txt  dist/oracle_to_mysql
cd dist
mv oracle_mig_mysql oracle_compare_mysql oracle_to_mysql
rm -rf oracle_mig_mysql.zip
zip -r oracle_mig_mysql.zip oracle_to_mysql
mv oracle_mig_mysql.zip /Users/kay/Documents/release/oracle_mig_mysql_MacOS.zip
# cp Macos release to lenovo MAC 192.168.149.129 for test
scp /Users/kay/Documents/python_code/oracle_to_mysql/dist/oracle_to_mysql/oracle_*_mysql kay@192.168.149.129:/Users/kay/Documents/oracle_to_mysql
# pack Windows release zip file
ssh administrator@192.168.149.33 "cd C:\PycharmProjects\python_code\oracle_to_mysql && pack.bat"
# cp Windows release file to MacOS 192.168.149.79
scp administrator@192.168.149.33:C:/PycharmProjects/python_code/oracle_to_mysql/oracle_mig_mysql.zip /Users/kay/Documents/release/oracle_mig_mysql_win_x86.zip
scp administrator@192.168.149.33:C:/PycharmProjects/python_code/oracle_to_mysql/oracle_mig_mysql/oracle_compare_mysql.exe /Users/kay/Documents/release/
scp administrator@192.168.149.33:C:/PycharmProjects/python_code/oracle_to_mysql/oracle_mig_mysql/oracle_mig_mysql.exe /Users/kay/Documents/release/
# cp Windows release file to local vmware 192.168.125.128 for test
ip=192.168.125.128
ping -c1 $ip >/dev/null 2>&1
if [ $? -eq 0 ]
then
    echo "$ip is alive,cp file to vm win10"
    scp /Users/kay/Documents/release/oracle*.exe administrator@192.168.125.128:C:/test/oracle_mig_mysql
else
    echo "$ip is down maybe vm is not open"
fi

# print test info
echo "you can test release MacOS 10.13.6 on ssh kay@192.168.149.129 cd /Users/kay/Documents/oracle_to_mysql"
echo "you can test release CentOS 7.9 on ssh root@192.168.189.200 cd /opt/oracle_to_mysql"
echo "you can test release Windows 10 on ssh administrator@192.168.125.128 cd C:\\\test\\oracle_mig_mysql"