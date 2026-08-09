package token

import (
	"context"
	"errors"
	"testing"

	serrors "github.com/appootb/substratum/v2/errors"
	"github.com/appootb/substratum/v2/proto/go/common"
	"github.com/appootb/substratum/v2/proto/go/secret"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/structpb"
)

type stubTokenClient struct {
	info *secret.Info
	err  error
	// captured inputs for assertions
	token string
	ctx   context.Context
}

func (s *stubTokenClient) Parse(ctx context.Context, in *structpb.Value, _ ...grpc.CallOption) (*secret.Info, error) {
	s.ctx = ctx
	if in != nil {
		s.token = in.GetStringValue()
	}
	return s.info, s.err
}

func TestParse_EmptyToken(t *testing.T) {
	a := &rpcAuth{}
	_, err := a.Parse(&common.Metadata{})
	if err == nil {
		t.Fatal("expected error for empty token")
	}
	if got := serrors.ErrorCode(err); got != int32(codes.Unauthenticated) {
		t.Fatalf("ErrorCode = %v, want Unauthenticated", got)
	}
	if got := err.Error(); got == "" {
		t.Fatal("expected non-empty error message")
	}
}

func TestParse_NilMetadata(t *testing.T) {
	// protobuf getters are nil-safe; treat as empty token.
	a := &rpcAuth{}
	_, err := a.Parse(nil)
	if err == nil {
		t.Fatal("expected error for nil metadata")
	}
	if got := serrors.ErrorCode(err); got != int32(codes.Unauthenticated) {
		t.Fatalf("ErrorCode = %v, want Unauthenticated", got)
	}
}

func TestParse_DelegatesToAccountRPC(t *testing.T) {
	// Mutates package hooks; do not run in parallel with other hook tests.
	want := &secret.Info{Account: 42, KeyId: 7}
	stub := &stubTokenClient{info: want}

	prevConn := getAccountConn
	prevClient := newTokenClient
	prevMD := withRPCMetadata
	t.Cleanup(func() {
		getAccountConn = prevConn
		newTokenClient = prevClient
		withRPCMetadata = prevMD
	})

	getAccountConn = func() grpc.ClientConnInterface { return nil }
	newTokenClient = func(grpc.ClientConnInterface) tokenParseClient { return stub }
	withRPCMetadata = func(md *common.Metadata, keyID int64) context.Context {
		if keyID != 99 {
			t.Fatalf("serverKeyID = %d, want 99", keyID)
		}
		return context.WithValue(context.Background(), struct{}{}, md.GetToken())
	}

	a := &rpcAuth{serverKeyID: 99}
	got, err := a.Parse(&common.Metadata{Token: "client-token"})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got != want {
		t.Fatalf("Parse info = %#v, want %#v", got, want)
	}
	if stub.token != "client-token" {
		t.Fatalf("RPC token = %q, want client-token", stub.token)
	}
	if stub.ctx == nil {
		t.Fatal("expected non-nil RPC context")
	}
}

func TestParse_PropagatesRPCError(t *testing.T) {
	// Mutates package hooks; do not run in parallel with other hook tests.
	rpcErr := errors.New("account unavailable")
	stub := &stubTokenClient{err: rpcErr}

	prevConn := getAccountConn
	prevClient := newTokenClient
	prevMD := withRPCMetadata
	t.Cleanup(func() {
		getAccountConn = prevConn
		newTokenClient = prevClient
		withRPCMetadata = prevMD
	})

	getAccountConn = func() grpc.ClientConnInterface { return nil }
	newTokenClient = func(grpc.ClientConnInterface) tokenParseClient { return stub }
	withRPCMetadata = func(*common.Metadata, int64) context.Context {
		return context.Background()
	}

	_, err := (&rpcAuth{}).Parse(&common.Metadata{Token: "x"})
	if !errors.Is(err, rpcErr) {
		t.Fatalf("err = %v, want %v", err, rpcErr)
	}
}

func TestInitServerRPCKey(t *testing.T) {
	prev := impl.serverKeyID
	t.Cleanup(func() { impl.serverKeyID = prev })

	InitServerRPCKey(12345)
	if impl.serverKeyID != 12345 {
		t.Fatalf("serverKeyID = %d, want 12345", impl.serverKeyID)
	}
}
