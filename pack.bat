rd /s /q oracle_mig_mysql __pycache__ build
del /f /s /q *.spec oracle_mig_mysql*.zip



C:\miniconda3\envs\python_code\Scripts\pyinstaller oracle_mig_mysql.py -F --clean --distpath oracle_mig_mysql
C:\miniconda3\envs\python_code\Scripts\pyinstaller oracle_compare_mysql.py -F --clean --distpath oracle_mig_mysql

copy /y config.ini oracle_mig_mysql\
copy /y custom_table.txt oracle_mig_mysql\
rem type nul > oracle_mig_mysql\custom_table.txt
ROBOCOPY oracle_client oracle_mig_mysql/oracle_client /E


"C:\Program Files\WinRAR\winRar.exe" a -k -m1 -ep1 -afzip -r -o+ oracle_mig_mysql.zip oracle_mig_mysql

