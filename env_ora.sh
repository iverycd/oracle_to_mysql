#!/bin/bash
cp /root/.bash_profile /root/.bash_profile.bak
echo "export ORACLE_HOME=/opt/oracle_to_mysql/ora_client/instantclient_11_2" >> /root/.bash_profile
echo "export LD_LIBRARY_PATH=\$ORACLE_HOME:\$LD_LIBRARY_PATH"  >> /root/.bash_profile
echo "export PATH=\$ORACLE_HOME:\$PATH" >> /root/.bash_profile