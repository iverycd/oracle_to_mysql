rm -rf /opt/python_code/oracle_to_mysql/oracle_to_mysql/*
pyinstaller oracle_mig_mysql.py -F --clean --additional-hooks-dir hooks --distpath oracle_to_mysql
pyinstaller oracle_compare_mysql.py -F --clean --additional-hooks-dir hooks --distpath oracle_to_mysql
cp config.ini custom_table.txt env_ora.sh oracle_to_mysql
cp -apr ora_client oracle_to_mysql
rm -rf oracle_mig_mysql.zip
zip -r oracle_mig_mysql.zip oracle_to_mysql