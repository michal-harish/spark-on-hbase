#!/bin/bash

DRIVER=2g
WORKERS=256
MEMORY="1g"
OVERHEAD="512"
EXTRAS="-XX:PermSize=64M -XX:MaxPermSize=256M"

while [[ $# -gt 0 ]] && [[ ."$1" = .-* ]] ;
do
    opt="$1";
    case "$1" in
        -e|--experimental)
        DRIVER="8g"
        WORKERS=512
        MEMORY="4096m"
        OVERHEAD="512"
        shift;
        ;;
        -xs|--extra-small)
        DRIVER="3g"
        WORKERS=256
        MEMORY="128"
        OVERHEAD="384"
        shift;
        ;;
        -s|--small)
        DRIVER="3g"
        WORKERS=512
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
        DRIVER="5g"
        WORKERS=512
        MEMORY="4672m"
        OVERHEAD="400"
        shift;
        ;;
        -xxl|--extra-extra-large)
        DRIVER="8g"
        WORKERS=720
        MEMORY="5644m"
        OVERHEAD="500"
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

echo "$DRIVER + $WORKERS x ($MEMORY + $OVERHEAD)  $EXTRAS"

