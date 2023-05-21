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

.PHONY: test-integration
test-integration:
	go test -race --tags=integration ./...

.PHONY: up
up:
	docker-compose up -d

.PHONY: down
down:
	docker-compose down