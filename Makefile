default: test

test: test-unit

test-unit: dependencies
	go test -race -cpu=1,2,4 -bench . -v ./...

test-integration: dependencies
	go test -tags integration -race -cpu=1,2,4 -bench . -v ./...

dependencies:
	go mod download

docs:
	GOPATH=$(pwd) GOMOD="$(pwd)/go.mod" godoc -http=:6060