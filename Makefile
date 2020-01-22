default: test

test: test-unit

test-unit: dependencies
	go test -v ./...

test-integration: dependencies
	go test -tags integration -v ./...

dependencies:
	go mod download

docs:
	GOPATH=$(pwd) GOMOD="$(pwd)/go.mod" godoc -http=:6060