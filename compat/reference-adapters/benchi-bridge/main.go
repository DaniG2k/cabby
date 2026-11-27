// Command benchi-bridge is a test-only reference adapter
// for Benchi (etc/benchi), a separate Go module that the shared bridge cannot import.
// It exposes exported Benchi functions and methods over the JSON-lines protocol in
// protocol.go. Each Rust test starts its own bridge process, so Benchi's
// one-server-per-process limit (cli.NewRunner mutates package globals) holds per
// test.
package main

func main() { brServe() }
