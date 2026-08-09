// Package token provides an RPC-backed client token parser for substratum auth.
//
// On import it registers auth.AlgorithmAuth with:
//   - client tokens: this package (account Token.Parse RPC)
//   - server tokens: substratum JWT plugin (local parse)
//
// Call InitServerRPCKey before serving so outbound Parse RPCs use the correct
// server signing key when attaching service-to-service metadata.
package token

import (
	"context"

	"github.com/appootb/protobuf/go/account"
	"github.com/appootb/protobuf/go/global"
	"github.com/appootb/substratum/v2/auth"
	"github.com/appootb/substratum/v2/client"
	"github.com/appootb/substratum/v2/errors"
	"github.com/appootb/substratum/v2/plugin/token"
	"github.com/appootb/substratum/v2/proto/go/common"
	"github.com/appootb/substratum/v2/proto/go/secret"
	"github.com/appootb/substratum/v2/util/valuepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/structpb"
)

// tokenParseClient is the subset of account.TokenClient used by Parse.
type tokenParseClient interface {
	Parse(ctx context.Context, in *structpb.Value, opts ...grpc.CallOption) (*secret.Info, error)
}

var (
	impl = &rpcAuth{}

	// Hooks for unit tests; production uses defaults.
	getAccountConn = func() grpc.ClientConnInterface {
		return client.Implementor().Get(global.Component_COMPONENT_ACCOUNT.String())
	}
	newTokenClient = func(cc grpc.ClientConnInterface) tokenParseClient {
		return account.NewTokenClient(cc)
	}
	withRPCMetadata = client.WithMetadata
)

func init() {
	// Client path: remote account service. Server path: local JWT verification.
	auth.RegisterImplementor(auth.NewAlgorithmAuth(impl, &token.JwtToken{}))
}

// InitServerRPCKey sets the key ID used when generating the outbound server
// token attached to Token.Parse RPC metadata.
func InitServerRPCKey(serverKeyID int64) {
	impl.serverKeyID = serverKeyID
}

// rpcAuth implements auth.TokenParser by delegating to account Token.Parse.
type rpcAuth struct {
	serverKeyID int64
}

// Parse validates that metadata carries a token, then asks the account service
// to resolve it into secret.Info.
//
// Requires client.Implementor() (and discovery/balancer for COMPONENT_ACCOUNT)
// to be registered before use. Empty tokens return codes.Unauthenticated.
func (m *rpcAuth) Parse(md *common.Metadata) (*secret.Info, error) {
	if md.GetToken() == "" {
		return nil, errors.New(codes.Unauthenticated, "empty token")
	}
	cc := getAccountConn()
	ctx := withRPCMetadata(md, m.serverKeyID)
	return newTokenClient(cc).Parse(ctx, valuepb.StringValue(md.GetToken()))
}
