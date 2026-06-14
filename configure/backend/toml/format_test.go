package toml

import (
	"reflect"
	"testing"
)

func TestEncodeDecodeKVs(t *testing.T) {
	input := map[string]string{
		"config/myapp/QQAppID": `{"type":"configure.Map","schema":"","value":"com.example:123","comment":"QQ App ID"}`,
		"config/myapp/Host":    `{"type":"string","schema":"","value":"localhost","comment":"host"}`,
		"config/myapp/DB/Port": `{"type":"int","schema":"","value":"5432","comment":"database port"}`,
	}
	//
	data, err := encodeKVs(input)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("encoded:\n%s", string(data))
	//
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
	//
	kvs, err := decodeKVs(data)
	if err != nil {
		t.Fatal(err)
	}
	if kvs["config/myapp/Host"] == "" {
		t.Fatal("expected legacy flat key")
	}
}
