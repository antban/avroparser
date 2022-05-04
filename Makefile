VERSION         := snapshot

GITHEAD         := $(shell git rev-parse HEAD)
PACKAGES        := $(shell find . -name '*.go' | xargs -n1 dirname | sort -u)
TESTFLAGS       := -v -race -count=1 -mod=readonly
BUILDFLAGS      := -mod=readonly -a
LINKFLAGS       := -X main.Version=$(VERSION) -X main.GitHead=$(GITHEAD)

default: build

check_fmt:
        @fmtRes=$$(gofmt -l .); \
        if [ -n "$${fmtRes}" ]; then \
                echo "gofmt checking failed!"; echo "$${fmtRes}"; echo; \
                exit 1; \
        fi

generate:
	go generate $(PACKAGES)

test: generate
	go test $(TESTFLAGS) $(PACKAGES)

build: check_fmt test
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build $(BUILDFLAGS) -ldflags="$(LINKFLAGS)" -o deploy/build/avro-convert ./cmd/avro-convert

clean:
	rm -rf deploy/build  && find -name mocks -type d -exec rm -rf "{}" +

.PHONY: clean generate check_fmt build
