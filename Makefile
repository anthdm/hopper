build:
	@go build -o bin/hopper cmd/main.go

run: build
	@./bin/hopper

test:
	@go test -v ./...
