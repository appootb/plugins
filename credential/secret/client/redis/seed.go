package redis

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"time"

	"github.com/appootb/protobuf/go/code"
	"github.com/appootb/substratum/credential"
	"github.com/appootb/substratum/errors"
	"github.com/appootb/substratum/storage"
	"github.com/go-redis/redis/v8"
)

const (
	UserSecretSeedKey = "account:secret:seed:%d:hash"
)

var (
	Seed = &seed{
		ctx:    context.Background(),
		expire: time.Hour * 24 * 7,
	}
)

func init() {
	credential.RegisterClientImplementor(Seed)
}

func InitRedis(s storage.Storage) {
	Seed.caches = s.GetRedisz()
}

type seed struct {
	ctx    context.Context
	expire time.Duration
	caches []redis.Cmdable
}

func (s *seed) getRedis(accountID uint64) redis.Cmdable {
	return s.caches[accountID%uint64(len(s.caches))]
}

// Add a new secret key.
func (s *seed) Add(accountID uint64, keyID int64, val []byte) error {
	cache := s.getRedis(accountID)
	key := fmt.Sprintf(UserSecretSeedKey, accountID)
	field := strconv.FormatInt(keyID, 10)
	v := base64.StdEncoding.EncodeToString(val)
	if err := cache.HSet(s.ctx, key, field, v).Err(); err != nil {
		return err
	}
	return cache.Expire(s.ctx, key, s.expire).Err()
}

// Get secret key.
func (s *seed) Get(accountID uint64, keyID int64) ([]byte, error) {
	key := fmt.Sprintf(UserSecretSeedKey, accountID)
	field := strconv.FormatInt(keyID, 10)
	v, err := s.getRedis(accountID).HGet(s.ctx, key, field).Result()
	if storage.IsEmpty(err) {
		return nil, errors.New(code.Error_ACCOUNT_LOGIN_REQUIRED, "")
	} else if err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(v)
}

// Revoke the secret key of the specified ID.
func (s *seed) Revoke(accountID uint64, keyID int64) error {
	key := fmt.Sprintf(UserSecretSeedKey, accountID)
	field := strconv.FormatInt(keyID, 10)
	return s.getRedis(accountID).HDel(s.ctx, key, field).Err()
}

// Revoke all secret keys of the specified account ID.
func (s *seed) RevokeAll(accountID uint64) error {
	key := fmt.Sprintf(UserSecretSeedKey, accountID)
	return s.getRedis(accountID).Del(s.ctx, key).Err()
}
