package token

import (
	"github.com/appootb/protobuf/go/account"
	"github.com/appootb/protobuf/go/global"
	"github.com/appootb/substratum/v2/auth"
	"github.com/appootb/substratum/v2/client"
	"github.com/appootb/substratum/v2/errors"
	"github.com/appootb/substratum/v2/plugin/token"
	"github.com/appootb/substratum/v2/proto/go/common"
	"github.com/appootb/substratum/v2/proto/go/secret"
	"github.com/appootb/substratum/v2/util/valuepb"
	"google.golang.org/grpc/codes"
)

var (
	Auth = &rpcAuth{}
)

func init() {
	auth.RegisterImplementor(auth.NewAlgorithmAuth(Auth, &token.JwtToken{}))
}

func InitKeyID(serverKeyID int64) {
	Auth.serverKeyID = serverKeyID
}

type rpcAuth struct {
	serverKeyID int64
}

func (m *rpcAuth) Parse(md *common.Metadata) (*secret.Info, error) {
	if md.GetToken() == "" {
		return nil, errors.New(codes.Unauthenticated, "empty token")
	}
	//
	cc := client.Implementor().Get(global.Component_COMPONENT_ACCOUNT.String())
	ctx := client.WithMetadata(md, m.serverKeyID)
	return account.NewTokenClient(cc).Parse(ctx, valuepb.StringValue(md.GetToken()))
}
