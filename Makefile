build:
	@go build -o bin/hopper cmd/main.go

run: build
	@rm -f default.hopper
	@./bin/hopper

test:
	@go test -v ./...
