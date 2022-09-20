package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/appootb/substratum/v2/credential"
	"github.com/appootb/substratum/v2/errors"
	"github.com/appootb/substratum/v2/storage"
	"github.com/go-redis/redis/v8"
	"google.golang.org/grpc/codes"
)

const (
	UserSecretSeedKey = "account:secret:seed:%d:hash"
)

var (
	impl = &seed{
		ctx:       context.Background(),
		component: os.Getenv("COMPONENT"),
	}
)

func init() {
	credential.RegisterClientImplementor(impl)
}

func InitComponent(component string) {
	impl.component = component
}

type seedInfo struct {
	PrivateKey  []byte
	NotBefore   time.Time
	NotAfter    time.Time
	LockMessage string
}

func parseSeedInfo(v string) *seedInfo {
	var info seedInfo
	_ = json.Unmarshal([]byte(v), &info)
	return &info
}

func (s *seedInfo) String() string {
	v, _ := json.Marshal(s)
	return string(v)
}

type seed struct {
	ctx       context.Context
	component string
}

func (s *seed) getRedis(accountID uint64) redis.Cmdable {
	return storage.Implementor().Get(s.component).GetRedis(accountID)
}

// Add a new secret key.
func (s *seed) Add(accountID uint64, keyID int64, val []byte, expire time.Duration) error {
	cache := s.getRedis(accountID)
	key := fmt.Sprintf(UserSecretSeedKey, accountID)
	field := strconv.FormatInt(keyID, 10)
	info := &seedInfo{
		PrivateKey: val,
		NotAfter:   time.Now().Add(expire),
	}
	if err := cache.HSet(s.ctx, key, field, info.String()).Err(); err != nil {
		return err
	}
	return cache.Expire(s.ctx, key, expire).Err()
}

// Refresh gets and refreshes the secret key's expiration.
func (s *seed) Refresh(accountID uint64, keyID int64, expire time.Duration) ([]byte, error) {
	cache := s.getRedis(accountID)
	key := fmt.Sprintf(UserSecretSeedKey, accountID)
	field := strconv.FormatInt(keyID, 10)
	v, err := s.getRedis(accountID).HGet(s.ctx, key, field).Result()
	if storage.IsEmpty(err) {
		return nil, errors.New(codes.Unauthenticated, "Unauthenticated")
	} else if err != nil {
		return nil, err
	}
	// Update expiration
	info, err := s.parseInfo(v)
	if err != nil {
		return nil, err
	}
	info.NotAfter = time.Now().Add(expire)
	if err = cache.HSet(s.ctx, key, field, info.String()).Err(); err != nil {
		return nil, err
	}
	if err = cache.Expire(s.ctx, key, expire).Err(); err != nil {
		return nil, err
	}
	return info.PrivateKey, nil
}

// Get secret key.
func (s *seed) Get(accountID uint64, keyID int64) ([]byte, error) {
	key := fmt.Sprintf(UserSecretSeedKey, accountID)
	field := strconv.FormatInt(keyID, 10)
	v, err := s.getRedis(accountID).HGet(s.ctx, key, field).Result()
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

// Revoke the secret key of the specified ID.
func (s *seed) Revoke(accountID uint64, keyID int64) error {
	key := fmt.Sprintf(UserSecretSeedKey, accountID)
	field := strconv.FormatInt(keyID, 10)
	return s.getRedis(accountID).HDel(s.ctx, key, field).Err()
}

// RevokeAll removes all secret keys of the specified user ID.
func (s *seed) RevokeAll(accountID uint64) error {
	key := fmt.Sprintf(UserSecretSeedKey, accountID)
	return s.getRedis(accountID).Del(s.ctx, key).Err()
}

// Lock all secret keys for a specified duration.
// Returns codes.FailedPrecondition (9).
func (s *seed) Lock(accountID uint64, reason string, duration time.Duration) error {
	key := fmt.Sprintf(UserSecretSeedKey, accountID)
	kvs, err := s.getRedis(accountID).HGetAll(s.ctx, key).Result()
	if err != nil {
		return err
	} else if len(kvs) == 0 {
		return nil
	}
	now := time.Now()
	vals := make([]interface{}, 0, len(kvs)*2)
	for field, val := range kvs {
		info := parseSeedInfo(val)
		info.NotBefore = now.Add(duration)
		info.LockMessage = reason
		vals = append(vals, field, info.String())
	}
	return s.getRedis(accountID).HMSet(s.ctx, key, vals...).Err()
}

// Unlock secret keys.
func (s *seed) Unlock(accountID uint64) error {
	key := fmt.Sprintf(UserSecretSeedKey, accountID)
	kvs, err := s.getRedis(accountID).HGetAll(s.ctx, key).Result()
	if err != nil {
		return err
	} else if len(kvs) == 0 {
		return nil
	}
	vals := make([]interface{}, 0, len(kvs)*2)
	for field, val := range kvs {
		info := parseSeedInfo(val)
		info.NotBefore = time.Unix(0, 0)
		info.LockMessage = ""
		vals = append(vals, field, info.String())
	}
	return s.getRedis(accountID).HMSet(s.ctx, key, vals...).Err()
}

func (s *seed) parseInfo(v string) (*seedInfo, error) {
	dt := time.Now()
	info := parseSeedInfo(v)
	if info.NotAfter.IsZero() {
		return nil, errors.New(codes.Unauthenticated, "empty secret:"+v)
	}
	if dt.After(info.NotAfter) {
		return nil, errors.New(codes.Unauthenticated, "secret expired")
	}
	if !info.NotBefore.IsZero() && dt.Before(info.NotBefore) {
		return nil, errors.New(codes.FailedPrecondition, info.LockMessage)
	}
	return info, nil
}
