package redis

import (
	"fmt"
	"strings"
	"testing"
	"time"

	serrors "github.com/appootb/substratum/v2/errors"
	"google.golang.org/grpc/codes"
)

func TestParseSeedInfoRoundTrip(t *testing.T) {
	in := &seedInfo{
		PrivateKey:  []byte("secret-bytes"),
		NotBefore:   time.Unix(100, 0).UTC(),
		NotAfter:    time.Unix(200, 0).UTC(),
		LockMessage: "banned",
	}
	out, err := parseSeedInfo(in.String())
	if err != nil {
		t.Fatal(err)
	}
	if string(out.PrivateKey) != "secret-bytes" {
		t.Fatalf("PrivateKey = %q", out.PrivateKey)
	}
	if out.LockMessage != "banned" {
		t.Fatalf("LockMessage = %q", out.LockMessage)
	}
	if !out.NotAfter.Equal(in.NotAfter) {
		t.Fatalf("NotAfter = %v", out.NotAfter)
	}
}

func TestParseSeedInfo_InvalidJSON(t *testing.T) {
	if _, err := parseSeedInfo("not-json"); err == nil {
		t.Fatal("expected error")
	}
}

func TestParseInfo_Expired(t *testing.T) {
	s := &seed{}
	info := &seedInfo{
		PrivateKey: []byte("k"),
		NotAfter:   time.Now().Add(-time.Minute),
	}
	_, err := s.parseInfo(info.String())
	if err == nil {
		t.Fatal("expected expired error")
	}
	if serrors.ErrorCode(err) != int32(codes.Unauthenticated) {
		t.Fatalf("code = %v", serrors.ErrorCode(err))
	}
}

func TestParseInfo_Locked(t *testing.T) {
	s := &seed{}
	info := &seedInfo{
		PrivateKey:  []byte("k"),
		NotAfter:    time.Now().Add(time.Hour),
		NotBefore:   time.Now().Add(time.Hour),
		LockMessage: "locked for abuse",
	}
	_, err := s.parseInfo(info.String())
	if err == nil {
		t.Fatal("expected lock error")
	}
	if serrors.ErrorCode(err) != int32(codes.FailedPrecondition) {
		t.Fatalf("code = %v", serrors.ErrorCode(err))
	}
	if !strings.Contains(err.Error(), "locked for abuse") {
		t.Fatalf("error should include lock message: %v", err)
	}
}

func TestParseInfo_Valid(t *testing.T) {
	s := &seed{}
	want := []byte("key-material")
	info := &seedInfo{
		PrivateKey: want,
		NotAfter:   time.Now().Add(time.Hour),
	}
	got, err := s.parseInfo(info.String())
	if err != nil {
		t.Fatal(err)
	}
	if string(got.PrivateKey) != string(want) {
		t.Fatalf("PrivateKey = %q", got.PrivateKey)
	}
}

func TestParseInfo_EmptyNotAfter(t *testing.T) {
	s := &seed{}
	info := &seedInfo{PrivateKey: []byte("k")}
	_, err := s.parseInfo(info.String())
	if err == nil {
		t.Fatal("expected empty secret error")
	}
	if serrors.ErrorCode(err) != int32(codes.Unauthenticated) {
		t.Fatalf("code = %v", serrors.ErrorCode(err))
	}
}

func TestInitComponent(t *testing.T) {
	prev := impl.component
	t.Cleanup(func() { impl.component = prev })
	InitComponent("account-cache")
	if impl.component != "account-cache" {
		t.Fatalf("component = %q", impl.component)
	}
}

func TestUserSecretSeedKeyFormat(t *testing.T) {
	// Document expected Redis key shape used by ops tooling.
	if got := fmt.Sprintf(UserSecretSeedKey, uint64(42)); got != "account:secret:seed:42:hash" {
		t.Fatalf("key = %q", got)
	}
}
