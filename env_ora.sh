#!/bin/bash
workdir=$(cd $(dirname $0); pwd)
echo "export ORACLE_HOME=$workdir/"oracle_client
echo "export LD_LIBRARY_PATH=\$ORACLE_HOME:\$LD_LIBRARY_PATH"