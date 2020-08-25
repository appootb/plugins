package account

import (
	"context"

	"github.com/appootb/protobuf/go/account"
	"github.com/appootb/protobuf/go/common"
	"github.com/appootb/protobuf/go/secret"
	"github.com/appootb/substratum/auth"
	"github.com/appootb/substratum/client"
	"github.com/appootb/substratum/token"
)

var (
	Auth = &rpcAuth{}
)

func init() {
	auth.RegisterImplementor(auth.NewAlgorithmAuth(Auth, token.Implementor()))
}

func InitKeyID(keyID int64) {
	Auth.keyID = keyID
}

type rpcAuth struct {
	keyID int64
}

func (n *rpcAuth) Parse(token string) (*secret.Info, error) {
	cc := client.Implementor().Get(common.Component_COMPONENT_ACCOUNT.String())
	ctx := client.WithContext(context.Background(), n.keyID)
	return account.NewInnerSecretClient(cc).GetSecretInfo(ctx, &account.Secret{
		Token: &token,
	})
}
