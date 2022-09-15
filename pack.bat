rd /s /q oracle_mig_mysql __pycache__ build
del /f /s /q *.spec oracle_mig_mysql*.zip
rem set /p version=请输入此次打包的版本号:


C:\miniconda3\envs\python_code\Scripts\pyinstaller oracle_mig_mysql.py -F --clean --distpath oracle_mig_mysql
C:\miniconda3\envs\python_code\Scripts\pyinstaller oracle_compare_mysql.py -F --clean --distpath oracle_mig_mysql

copy /y config.ini oracle_mig_mysql\
type nul > oracle_mig_mysql\custom_table.txt
ROBOCOPY oracle_client oracle_mig_mysql/oracle_client /E
rem copy /y config.ini dist\mysql_mig_kingbase_%version%
rem type nul > dist\mysql_mig_kingbase_%version%\custom_table.txt

rem cd dist\mysql_mig_kingbase_%version%
rem ren mysql_mig_kingbase_%version%.exe mysql_mig_kingbase.exe
rem cd ..

"C:\Program Files\WinRAR\winRar.exe" a -k -m1 -ep1 -afzip -r -o+ oracle_mig_mysql.zip oracle_mig_mysql

