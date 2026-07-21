package etp

import (
	"context"
	"testing"
)

func TestRouterGroupsInheritParentMiddleware(t *testing.T) {
	router := NewRouter()
	var calls []string
	if err := router.Use("*", func(next Handler) Handler {
		return func(ctx *Context) error {
			calls = append(calls, "root")
			return next(ctx)
		}
	}); err != nil {
		t.Fatalf("use root: %v", err)
	}
	group := router.Group("")
	if err := group.Use("*", func(next Handler) Handler {
		return func(ctx *Context) error {
			calls = append(calls, "group")
			return next(ctx)
		}
	}); err != nil {
		t.Fatalf("use group: %v", err)
	}
	if err := group.On("message.ping", func(ctx *Context) error {
		calls = append(calls, "handler")
		return nil
	}); err != nil {
		t.Fatalf("on: %v", err)
	}
	router.Compile()

	if err := router.Emit(&Context{Context: context.Background(), Event: "message.ping"}); err != nil {
		t.Fatalf("emit: %v", err)
	}
	want := []string{"root", "group", "handler"}
	if len(calls) != len(want) {
		t.Fatalf("calls = %v", calls)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("calls = %v", calls)
		}
	}
}

func BenchmarkRouterEmitMinimal(b *testing.B) {
	router := NewRouter()
	if err := router.On("message.ping", func(ctx *Context) error { return nil }); err != nil {
		b.Fatal(err)
	}
	router.Compile()
	ctx := &Context{Context: context.Background(), Event: "message.ping"}

	b.ReportAllocs()
	for b.Loop() {
		if err := router.Emit(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRouterEmitWildcards(b *testing.B) {
	router := NewRouter()
	if err := router.Use("message.*", func(next Handler) Handler { return next }); err != nil {
		b.Fatal(err)
	}
	if err := router.On("message.ping", func(ctx *Context) error { return nil }); err != nil {
		b.Fatal(err)
	}
	router.Compile()
	ctx := &Context{Context: context.Background(), Event: "message.ping"}

	b.ReportAllocs()
	for b.Loop() {
		if err := router.Emit(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
