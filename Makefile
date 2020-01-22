default: test

test: test-unit

test-unit:
	go test -v ./...

test-integration:
	go test -tags integration -v ./...

docs:
	GOPATH=$(pwd) GOMOD="$(pwd)/go.mod" godoc -http=:6060