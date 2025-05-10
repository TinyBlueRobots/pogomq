.PHONY: all clean lint test tidy format sqlc

all: sqlc lint format test

clean:
	go clean -cache	
	rm -f go.sum
	${MAKE} tidy

sqlc:
	go tool sqlc generate -f ./internal/sql/sqlc.yaml

lint:
	go tool golangci-lint run

format:
	go fmt ./...

test:
	go test -v ./...

tidy:
	go get -u && go mod tidy
	go get tool
