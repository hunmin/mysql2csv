## 1. 빌드 방법
```
$ go build -ldflags "-s -w" -o mysql2csv
```

## 2. 실행방법
```
$ ./mysql2csv -u id -p password -d test -q "SELECT * FROM csv_table" -sep ";" -o csvfile.csv -report 10
```