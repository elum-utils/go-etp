module github.com/elum-utils/go-etp/adapters/quic

go 1.25.0

require (
	github.com/elum-utils/go-etp v0.0.0
	github.com/quic-go/quic-go v0.60.0
)

require (
	golang.org/x/crypto v0.51.0 // indirect
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
)

replace github.com/elum-utils/go-etp => ../..
