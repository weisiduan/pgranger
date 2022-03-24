.PHONY: dockerup
dockerup:
	docker-compose -p userprofile -f docker/docker-compose.yml up -d --no-recreate

.PHONY: setup
setup: dockerup migrateup

.PHONY: migrateup
migrateup:
	for i in {3..6}; do migrate -database postgresql://pgranger_test:password@127.0.0.1:544$$i/pgranger_test?sslmode=disable -path schema_migration up; done

.PHONY: migratedown
migratedown:
	for i in {3..6}; do migrate -database postgresql://pgranger_test:password@127.0.0.1:544$$i/pgranger_test?sslmode=disable -path schema_migration down -all; done

.PHONY: teardown
teardown: migratedown dockerdown

.PHONY: dockerdown
dockerdown:
	docker-compose -p userprofile -f docker/docker-compose.yml down

.PHONY: sql
sql:
	psql --host localhost --port $(port) --dbname pgranger_test --user pgranger_test
