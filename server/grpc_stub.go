//go:build grpc

package main

// startGRPCServer is a placeholder for a gRPC server exposing the same
// Query service as the HTTP API. It is behind a build tag so that the
// regular build does not require gRPC dependencies.
func startGRPCServer(e *Engine) error {
	return nil
}
