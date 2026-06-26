package benchmarks

import (
	"context"
	"testing"

	. "github.com/elum-utils/go-etp"
)

func BenchmarkSessionUnifiedSmallRequest(b *testing.B) {
	config := DefaultSessionConfig(RoleClient)
	config.Payload.MaxInlineBodyBytes = 1024
	session := NewSessionWithConfig(benchTransport{}, config)
	opts := MessageOptions{
		Event: "attach.upload",
		Data:  []byte("hello"),
		Fields: []TransferField{
			{Key: "dialog", Value: "dialog-1"},
		},
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := session.Request(context.Background(), opts); err != nil {
			b.Fatal(err)
		}
	}
}
