.PHONY: build-agent build-test-client build-all clean

build-agent:
	cd agent_module && go build -o ../agent . && cd ..

build-test-client:
	cd cmd/test_client && go build -o ../../test_client . && cd ../..

build-all: build-agent build-test-client

clean:
	rm -f agent test_client
