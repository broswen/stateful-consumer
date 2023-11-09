

.PHONY: build compose


build:
	docker build . -f Dockerfile -t broswen/stateful-consumer:latest

compose:
	docker compose up --build