.PHONY: jaeger
jaeger: 
	docker run --name jaeger \
		-e COLLECTOR_OTLP_ENABLED=true \
		-p 16686:16686 \
		-p 4317:4317 \
		-p 4318:4318 \
	jaegertracing/all-in-one:1.35

.PHONY: test
test:
	go test -race ./...

.PHONY: up
up:
	docker-compose up -d

.PHONY: down
down:
	docker-compose down

.PHONY: gen
gen:
	cd example/a && protoc --go_out=./pb --go_opt=paths=source_relative \
		--go-grpc_out=./pb --go-grpc_opt=paths=source_relative \
		a.proto