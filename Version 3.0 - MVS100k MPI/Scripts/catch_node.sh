#!/bin/bash

. /etc/profile

export LD_LIBRARY_PATH=/home2/mpc1/SAT/lib:$LD_LIBRARY_PATH

cd /home2/mpc1/SAT
awk -f catch_node.awk