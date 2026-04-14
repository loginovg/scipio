.PHONY: lint tests testsuite ci

lint:
	golangci-lint run ./...

tests:
	go test -count=1 -tags test_dep ./...

testsuite:
	python3 -m pytest testsuite

ci: lint tests testsuite
