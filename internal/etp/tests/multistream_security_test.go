package tests

import (
	"context"
	"encoding/binary"
	. "github.com/elum-utils/go-etp/internal/etp"
	"net"
	"strings"
	"testing"
	"time"
)

func TestMultiStreamRejectsFrameChannelMismatch(t *testing.T) {
	transport := NewMultiStreamTransport(MultiStreamTransportConfig{Context: t.Context()})
	defer transport.Close()
	frame := NewFrame(FramePing, 0, nil)
	frame.Header.ChannelID = ChannelControl
	data := mustEncodeFrame(t, frame)
	if err := transport.SendFrameOnChannel(ChannelBulk, data); err == nil {
		t.Fatal("mismatched frame and stream channels were accepted")
	}
}

func TestMultiStreamRejectsUnknownAndDuplicateStreamChannels(t *testing.T) {
	accepts := make(chan DeadlineStream, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := NewMultiStreamTransport(MultiStreamTransportConfig{
		Context: ctx,
		Guard:   DefaultSlowlorisConfig(),
		AcceptStream: func(ctx context.Context) (DeadlineStream, error) {
			select {
			case stream := <-accepts:
				return stream, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	})
	defer transport.Close()

	local, remote := net.Pipe()
	accepts <- local
	go func() {
		var preface [2]byte
		binary.BigEndian.PutUint16(preface[:], ChannelBackground+1)
		_, _ = remote.Write(preface[:])
	}()
	if _, err := transport.ReadFrame(); err == nil || !strings.Contains(err.Error(), "invalid multistream channel") {
		t.Fatalf("unknown stream channel error = %v", err)
	}
	_ = remote.Close()

	firstLocal, firstRemote := net.Pipe()
	accepts <- firstLocal
	go writeChannelPreface(firstRemote, ChannelControl)
	time.Sleep(20 * time.Millisecond)
	secondLocal, secondRemote := net.Pipe()
	accepts <- secondLocal
	go writeChannelPreface(secondRemote, ChannelControl)
	if _, err := transport.ReadFrame(); err == nil || !strings.Contains(err.Error(), "duplicate incoming stream") {
		t.Fatalf("duplicate stream error = %v", err)
	}
	_ = firstRemote.Close()
	_ = secondRemote.Close()
}

func TestMultiStreamValidatesFrameAgainstStreamPreface(t *testing.T) {
	accepts := make(chan DeadlineStream, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := NewMultiStreamTransport(MultiStreamTransportConfig{
		Context: ctx,
		Guard:   DefaultSlowlorisConfig(),
		AcceptStream: func(context.Context) (DeadlineStream, error) {
			return <-accepts, nil
		},
	})
	defer transport.Close()
	local, remote := net.Pipe()
	accepts <- local
	go func() {
		writeChannelPreface(remote, ChannelBulk)
		frame := NewFrame(FramePing, 0, nil)
		frame.Header.ChannelID = ChannelControl
		data := mustEncodeFrame(t, frame)
		var prefix [4]byte
		binary.BigEndian.PutUint32(prefix[:], uint32(len(data)))
		_, _ = remote.Write(prefix[:])
		_, _ = remote.Write(data)
	}()
	if _, err := transport.ReadFrame(); err == nil || !strings.Contains(err.Error(), "does not match stream preface") {
		t.Fatalf("channel binding error = %v", err)
	}
	_ = remote.Close()
}

func TestMultiStreamCloseInterruptsPendingPreface(t *testing.T) {
	accepts := make(chan DeadlineStream, 1)
	guard := DefaultSlowlorisConfig()
	guard.LengthTimeout = 5 * time.Second
	transport := NewMultiStreamTransport(MultiStreamTransportConfig{
		Context: t.Context(),
		Guard:   guard,
		AcceptStream: func(ctx context.Context) (DeadlineStream, error) {
			select {
			case stream := <-accepts:
				return stream, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	})
	local, remote := net.Pipe()
	defer remote.Close()
	accepts <- local
	time.Sleep(20 * time.Millisecond)

	done := make(chan error, 1)
	go func() { done <- transport.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("close waited for slowloris preface deadline")
	}
}

func writeChannelPreface(conn net.Conn, channel uint16) {
	var preface [2]byte
	binary.BigEndian.PutUint16(preface[:], channel)
	_, _ = conn.Write(preface[:])
}
