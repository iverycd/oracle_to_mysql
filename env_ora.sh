#!/bin/bash
workdir=$(cd $(dirname $0); pwd)
echo "export ORACLE_HOME=$workdir/"oracle_client > run_env
echo "export LD_LIBRARY_PATH=\$ORACLE_HOME:\$LD_LIBRARY_PATH" >> run_env
echo "export LANG=en_US.UTF-8" >> run_env