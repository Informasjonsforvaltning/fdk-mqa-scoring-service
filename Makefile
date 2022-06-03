test:
	docker-compose up -d
	POSTGRES_HOST=localhost POSTGRES_PORT=5432 POSTGRES_USERNAME=postgres \
	POSTGRES_PASSWORD=postgres POSTGRES_DB_NAME=mqa \
	cargo test -- --test-threads 1
	docker-compose down -v
