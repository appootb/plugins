package console

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/appootb/substratum/v2/logger"
	"github.com/appootb/substratum/v2/proto/go/common"
	"github.com/appootb/substratum/v2/proto/go/secret"
)

func TestLog_LevelFilter(t *testing.T) {
	var out, errBuf bytes.Buffer
	l := &stdJSON{out: &out, err: &errBuf}
	l.UpdateLevel(logger.WarnLevel)

	l.Log(logger.InfoLevel, nil, "skip", nil)
	if out.Len() != 0 || errBuf.Len() != 0 {
		t.Fatalf("expected filtered log, out=%q err=%q", out.String(), errBuf.String())
	}

	l.Log(logger.WarnLevel, nil, "warn", nil)
	if out.Len() == 0 {
		t.Fatal("expected warn on stdout")
	}
}

func TestLog_ErrorToStderr(t *testing.T) {
	var out, errBuf bytes.Buffer
	l := &stdJSON{out: &out, err: &errBuf}

	l.Log(logger.ErrorLevel, nil, "boom", logger.Content{"x": 1})
	if out.Len() != 0 {
		t.Fatalf("error should not write stdout: %q", out.String())
	}
	if !strings.Contains(errBuf.String(), `"LEVEL":"ERROR"`) {
		t.Fatalf("stderr = %q", errBuf.String())
	}
	if !strings.Contains(errBuf.String(), `"MESSAGE":"boom"`) {
		t.Fatalf("stderr = %q", errBuf.String())
	}
}

func TestLog_FieldNormalization(t *testing.T) {
	var out bytes.Buffer
	l := &stdJSON{out: &out, err: &out}
	md := &common.Metadata{TraceId: "t1"}
	sec := &secret.Info{Account: 99}

	l.Log(logger.InfoLevel, md, "req", logger.Content{
		logger.LogPath:     "/v1/ping",
		logger.LogRequest:  map[string]string{"a": "b"},
		logger.LogResponse: "ok",
		logger.LogConsumed: 12,
		logger.LogSecret:   sec,
		logger.LogError:    "none",
		"custom":           "v",
	})

	var m map[string]interface{}
	if err := json.Unmarshal(out.Bytes(), &m); err != nil {
		t.Fatal(err)
	}
	if m["PATH"] != "/v1/ping" {
		t.Fatalf("PATH = %v", m["PATH"])
	}
	if m["UID"] != float64(99) { // json numbers are float64
		t.Fatalf("UID = %v", m["UID"])
	}
	if m["custom"] != "v" {
		t.Fatalf("custom = %v", m["custom"])
	}
	if m["ERROR"] != "none" {
		t.Fatalf("ERROR = %v", m["ERROR"])
	}
}

func TestLog_SecretTypeSafe(t *testing.T) {
	var out bytes.Buffer
	l := &stdJSON{out: &out, err: &out}
	// Non-*secret.Info must not panic.
	l.Log(logger.InfoLevel, nil, "x", logger.Content{logger.LogSecret: "not-a-secret"})
	if !strings.Contains(out.String(), `"UID":"not-a-secret"`) {
		t.Fatalf("out = %q", out.String())
	}
}
