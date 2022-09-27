rm -rf package/*.py
cp db_info.py package
cp configDB.py package
cp readConfig.py package
cp sql_format.py package

pyinstaller -F --clean -p package oracle_mig_mysql.py
pyinstaller -F --clean -p package oracle_compare_mysql.py
rm -rf dist/oracle_to_mysql
mkdir -p dist/oracle_to_mysql
cp -r oracle_client dist/oracle_to_mysql
cp config.ini custom_table.txt  dist/oracle_to_mysql
cd dist
mv oracle_mig_mysql oracle_compare_mysql oracle_to_mysql
rm -rf oracle_mig_mysql.zip
zip -r oracle_mig_mysql.zip oracle_to_mysql