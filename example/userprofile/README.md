Prerequisites:
```
First install golang-migrate CLI.
https://github.com/golang-migrate/migrate/tree/master/cmd/migrate

Also install docker.
```

Steps:
```
cd ~/pgranger/example/userprofile
make dockerup
make migrateup

go run main.go --configpath=$(pwd)/config.json

make migratedown
make dockerdown
```
