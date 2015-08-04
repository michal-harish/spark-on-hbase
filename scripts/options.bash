#!/bin/bash

DRIVER=2g
WORKERS=32
MEMORY="1g"
OVERHEAD="384"
EXTRAS="-XX:PermSize=64M -XX:MaxPermSize=256M"

while [[ $# -gt 0 ]] && [[ ."$1" = .-* ]] ;
do
    opt="$1";
    case "$1" in
        -s|--small)
        DRIVER="1g"
        WORKERS=256
        MEMORY="640m"
        OVERHEAD="384"
        shift;
        ;;
        -m|--medium)
        DRIVER="4g"
        WORKERS=512
        MEMORY="1664m"
        OVERHEAD="384"
        shift;
        ;;
        -l|--large)
        DRIVER="4g"
        WORKERS=300
        MEMORY="4672m"
        OVERHEAD="400"
        shift;
        ;;
        -xl|--extra-large)
        DRIVER="8g"
        WORKERS=512
        MEMORY="4096m"
        OVERHEAD="512"
        shift;
        ;;
        -p|--profile)
        EXTRAS="-XX:PermSize=64M -XX:MaxPermSize=256M -agentpath:/opt/jprofiler/bin/linux-x64/libjprofilerti.so=port=8849,nowait"
        shift;
        ;;
        *)
            echo "unknown option $1"
            exit 1;
            shift;
        ;;
    esac
done

echo "DRIVER: SPARK_LOCAL_IP = $SPARK_LOCAL_IP"
echo "DRIVER: SPARK_HOME = $SPARK_HOME"
echo "DRIVER: HADOOP_HOME = $HADOOP_HOME"
echo "DRIVER: HADOOP_CONF_DIR = $HADOOP_CONF_DIR"
echo "HBASE_CONF_DIR = $HBASE_CONF_DIR"
echo "EXECUTORS: SPARK_REMOTE_JAR = $SPARK_REMOTE_JAR"
echo "$DRIVER + $WORKERS x ($MEMORY + $OVERHEAD)  $EXTRAS"

