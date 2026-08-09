package toml

import (
	"reflect"
	"strings"
	"testing"
)

func TestEncodeDecodeKVs(t *testing.T) {
	input := map[string]string{
		"config/myapp/QQAppID": `{"type":"configure.Map","schema":"","value":"com.example:123","comment":"QQ App ID"}`,
		"config/myapp/Host":    `{"type":"string","schema":"","value":"localhost","comment":"host"}`,
		"config/myapp/DB/Port": `{"type":"int","schema":"","value":"5432","comment":"database port"}`,
	}

	data, err := encodeKVs(input)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "[config.myapp]") && !strings.Contains(string(data), "[config.myapp.DB]") {
		// hierarchical tables should appear in some form
		t.Logf("encoded:\n%s", string(data))
	}

	output, err := decodeKVs(data)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(input, output) {
		t.Fatalf("round-trip mismatch\ninput:  %#v\noutput: %#v", input, output)
	}
}

func TestDecodeLegacyFlat(t *testing.T) {
	data := []byte(`"config/myapp/Host" = "{\"type\":\"string\",\"schema\":\"\",\"value\":\"localhost\",\"comment\":\"host\"}"`)

	kvs, err := decodeKVs(data)
	if err != nil {
		t.Fatal(err)
	}
	if kvs["config/myapp/Host"] == "" {
		t.Fatal("expected legacy flat key")
	}
}

func TestDecodeEmpty(t *testing.T) {
	kvs, err := decodeKVs(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 0 {
		t.Fatalf("want empty map, got %#v", kvs)
	}
}

func TestDecodeHierarchicalNumericValue(t *testing.T) {
	// value written as bare TOML integer should still become string via toString.
	data := []byte(`
[config.myapp.Port]
type = "int"
value = 5432
comment = "port"
`)
	kvs, err := decodeKVs(data)
	if err != nil {
		t.Fatal(err)
	}
	item, err := parseItemJSON(kvs["config/myapp/Port"])
	if err != nil {
		t.Fatal(err)
	}
	if item.Value != "5432" {
		t.Fatalf("value = %q, want 5432", item.Value)
	}
	if item.Type != "int" {
		t.Fatalf("type = %q", item.Type)
	}
}

func TestEncodeInvalidJSON(t *testing.T) {
	_, err := encodeKVs(map[string]string{"a": "not-json"})
	if err == nil {
		t.Fatal("expected error for invalid ConfigItem JSON")
	}
}

func TestIsLegacyFlat(t *testing.T) {
	if isLegacyFlat(nil) || isLegacyFlat(map[string]string{}) {
		t.Fatal("empty should not be legacy flat")
	}
	if !isLegacyFlat(map[string]string{"k": `{"type":"string","value":"x"}`}) {
		t.Fatal("expected legacy flat")
	}
	if isLegacyFlat(map[string]string{"k": "plain"}) {
		t.Fatal("plain value is not legacy flat")
	}
}
