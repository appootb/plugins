// Package redis implements credential.Client using Redis hashes.
//
// Keys are stored as HSET account:secret:seed:{uid}:hash with field = keyID
// and value = JSON seedInfo (private key material + validity window + lock).
//
// Blank-import registers the client implementor. Call InitComponent when the
// Redis component name is not provided via the COMPONENT env var.
package redis

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	sctx "github.com/appootb/substratum/v2/context"
	"github.com/appootb/substratum/v2/credential"
	"github.com/appootb/substratum/v2/errors"
	"github.com/appootb/substratum/v2/storage"
	"github.com/go-redis/redis/v8"
	"google.golang.org/grpc/codes"
)

const (
	// UserSecretSeedKey is the Redis hash key format for account secret seeds.
	UserSecretSeedKey = "account:secret:seed:%d:hash"
)

var (
	impl = &seed{
		component: os.Getenv("COMPONENT"),
	}
)

func init() {
	credential.RegisterClientImplementor(impl)
}

// InitComponent overrides the storage component used to resolve Redis shards.
func InitComponent(component string) {
	impl.component = component
}

// seedInfo is the JSON payload stored per keyID field.
type seedInfo struct {
	PrivateKey  []byte    `json:"PrivateKey"`
	NotBefore   time.Time `json:"NotBefore"`
	NotAfter    time.Time `json:"NotAfter"`
	LockMessage string    `json:"LockMessage"`
}

func parseSeedInfo(v string) (*seedInfo, error) {
	var info seedInfo
	if err := json.Unmarshal([]byte(v), &info); err != nil {
		return nil, err
	}
	return &info, nil
}

func (s *seedInfo) String() string {
	v, _ := json.Marshal(s)
	return string(v)
}

type seed struct {
	component string
	// redisFor resolves the Redis client for an account (overridable in tests).
	redisFor func(accountID uint64) redis.Cmdable
}

func (s *seed) getRedis(accountID uint64) redis.Cmdable {
	if s.redisFor != nil {
		return s.redisFor(accountID)
	}
	return storage.Implementor().Get(s.component).GetRedis(accountID)
}

// Add creates a new secret key field and refreshes the hash TTL.
func (s *seed) Add(accountID uint64, keyID int64, val []byte, expire time.Duration) error {
	cache := s.getRedis(accountID)
	key := fmt.Sprintf(UserSecretSeedKey, accountID)
	field := strconv.FormatInt(keyID, 10)
	info := &seedInfo{
		PrivateKey: val,
		NotAfter:   time.Now().Add(expire),
	}
	if err := cache.HSet(sctx.Context(), key, field, info.String()).Err(); err != nil {
		return err
	}
	return cache.Expire(sctx.Context(), key, expire).Err()
}

// Refresh gets and refreshes the secret key's expiration.
func (s *seed) Refresh(accountID uint64, keyID int64, expire time.Duration) ([]byte, error) {
	cache := s.getRedis(accountID)
	key := fmt.Sprintf(UserSecretSeedKey, accountID)
	field := strconv.FormatInt(keyID, 10)
	v, err := cache.HGet(sctx.Context(), key, field).Result()
	if storage.IsEmpty(err) {
		return nil, errors.New(codes.Unauthenticated, "Unauthenticated")
	} else if err != nil {
		return nil, err
	}
	info, err := s.parseInfo(v)
	if err != nil {
		return nil, err
	}
	info.NotAfter = time.Now().Add(expire)
	if err = cache.HSet(sctx.Context(), key, field, info.String()).Err(); err != nil {
		return nil, err
	}
	if err = cache.Expire(sctx.Context(), key, expire).Err(); err != nil {
		return nil, err
	}
	return info.PrivateKey, nil
}

// Get secret key material for keyID, enforcing expiry and lock windows.
func (s *seed) Get(accountID uint64, keyID int64) ([]byte, error) {
	key := fmt.Sprintf(UserSecretSeedKey, accountID)
	field := strconv.FormatInt(keyID, 10)
	v, err := s.getRedis(accountID).HGet(sctx.Context(), key, field).Result()
	if storage.IsEmpty(err) {
		return nil, errors.New(codes.Unauthenticated, "Unauthenticated")
	} else if err != nil {
		return nil, err
	}
	info, err := s.parseInfo(v)
	if err != nil {
		return nil, err
	}
	return info.PrivateKey, nil
}

// Revoke removes the secret key of the specified ID.
func (s *seed) Revoke(accountID uint64, keyID int64) error {
	key := fmt.Sprintf(UserSecretSeedKey, accountID)
	field := strconv.FormatInt(keyID, 10)
	return s.getRedis(accountID).HDel(sctx.Context(), key, field).Err()
}

// RevokeAll removes all secret keys of the specified user ID.
func (s *seed) RevokeAll(accountID uint64) error {
	key := fmt.Sprintf(UserSecretSeedKey, accountID)
	return s.getRedis(accountID).Del(sctx.Context(), key).Err()
}

// Lock disables all secret keys for a duration (FailedPrecondition on Get).
// Non-positive duration locks for ~100 years.
func (s *seed) Lock(accountID uint64, reason string, duration time.Duration) error {
	key := fmt.Sprintf(UserSecretSeedKey, accountID)
	kvs, err := s.getRedis(accountID).HGetAll(sctx.Context(), key).Result()
	if err != nil {
		return err
	} else if len(kvs) == 0 {
		return nil
	}
	now := time.Now()
	if duration <= 0 {
		duration = time.Hour * 24 * 365 * 100
	}
	vals := make([]interface{}, 0, len(kvs)*2)
	for field, val := range kvs {
		info, err := parseSeedInfo(val)
		if err != nil {
			return err
		}
		info.NotBefore = now.Add(duration)
		info.LockMessage = reason
		vals = append(vals, field, info.String())
	}
	return s.getRedis(accountID).HMSet(sctx.Context(), key, vals...).Err()
}

// Unlock clears NotBefore / LockMessage on all secret keys for the account.
func (s *seed) Unlock(accountID uint64) error {
	key := fmt.Sprintf(UserSecretSeedKey, accountID)
	kvs, err := s.getRedis(accountID).HGetAll(sctx.Context(), key).Result()
	if err != nil {
		return err
	} else if len(kvs) == 0 {
		return nil
	}
	vals := make([]interface{}, 0, len(kvs)*2)
	for field, val := range kvs {
		info, err := parseSeedInfo(val)
		if err != nil {
			return err
		}
		info.NotBefore = time.Unix(0, 0)
		info.LockMessage = ""
		vals = append(vals, field, info.String())
	}
	return s.getRedis(accountID).HMSet(sctx.Context(), key, vals...).Err()
}

func (s *seed) parseInfo(v string) (*seedInfo, error) {
	dt := time.Now()
	info, err := parseSeedInfo(v)
	if err != nil {
		return nil, errors.New(codes.Unauthenticated, "invalid secret payload")
	}
	if info.NotAfter.IsZero() {
		return nil, errors.New(codes.Unauthenticated, "empty secret")
	}
	if dt.After(info.NotAfter) {
		return nil, errors.New(codes.Unauthenticated, "secret expired")
	}
	if !info.NotBefore.IsZero() && dt.Before(info.NotBefore) {
		return nil, errors.New(codes.FailedPrecondition, info.LockMessage)
	}
	return info, nil
}
