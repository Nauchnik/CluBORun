#!/bin/sh

export PATH=/home/zaikin/bin:/opt/cleo/bin:/common/intel/impi/4.0.1/bin64:/opt/intel/ics/2011.0.013/cc/bin/intel64:/usr/local/bin:/usr/bin:/bin:/opt/bin:/usr/x86_64-pc-linux-gnu/gcc-bin/4.5.2:/usr/x86_64-pc-linux-gnu/gcc-bin/3.4.6:/usr/libexec/gpc/x86_64-pc-linux-gnu/3.4
cd /home/zaikin/temp/BOINC
awk -f catch_node.awk