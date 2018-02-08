#! /bin/sh
#
# Usage: ./analyse.sh file-path 
#

if [ "$#" -ne 1 ]; then
    echo "$#"
    echo "Illegal number of parameters"
    echo "Usage: ./analyse.sh file-path"
    echo "Example: ./analyse.sh curl.json"
    exit 1
fi

curl -X POST -H "Content-Type: application/json" -H "Accept: text/plain" -d "@$1" -v "http://localhost:8080/wikiplag/rest/analyse"