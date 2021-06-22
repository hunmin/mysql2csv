#/bin/sh
BUILD=`date +%Y%m%d%H%M`
REVISION=`git rev-parse --short HEAD`
go build -ldflags "-s -w -X main.Build=$BUILD -X main.Revision=$REVISION" -o mysql2csv
