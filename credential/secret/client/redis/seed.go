package redis

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"time"

	"github.com/appootb/protobuf/go/code"
	"github.com/appootb/substratum/credential"
	"github.com/appootb/substratum/errors"
	"github.com/appootb/substratum/storage"
	"github.com/go-redis/redis"
)

const (
	UserSecretSeedKey = "account:secret:seed:%d:hash"
)

var (
	Seed = &seed{
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
	if err := cache.HSet(key, field, v).Err(); err != nil {
		return err
	}
	return cache.Expire(key, s.expire).Err()
}

// Get secret key.
func (s *seed) Get(accountID uint64, keyID int64) ([]byte, error) {
	key := fmt.Sprintf(UserSecretSeedKey, accountID)
	field := strconv.FormatInt(keyID, 10)
	v, err := s.getRedis(accountID).HGet(key, field).Result()
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
	return s.getRedis(accountID).HDel(key, field).Err()
}

// Revoke all secret keys of the specified account ID.
func (s *seed) RevokeAll(accountID uint64) error {
	key := fmt.Sprintf(UserSecretSeedKey, accountID)
	return s.getRedis(accountID).Del(key).Err()
}
