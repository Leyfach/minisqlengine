.PHONY: build test

build:
	cargo build --manifest-path core/Cargo.toml
	cd server && go build

test:
	cargo test --manifest-path core/Cargo.toml
	cd server && go test
