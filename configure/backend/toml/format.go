package toml

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	substratum "github.com/appootb/substratum/v2/plugin/configure"
	"github.com/pelletier/go-toml/v2"
)

// parseItemJSON unmarshals a ConfigItem JSON blob stored as a KV value.
func parseItemJSON(s string) (substratum.ConfigItem, error) {
	var item substratum.ConfigItem
	err := json.Unmarshal([]byte(s), &item)
	return item, err
}

// decodeKVs loads flat key→ConfigItemJSON map from file bytes.
//
// Supports:
//  1. Legacy flat form: quoted full paths as keys, JSON strings as values
//  2. Hierarchical form: nested tables with type/value leaf fields
func decodeKVs(data []byte) (map[string]string, error) {
	if len(data) == 0 {
		return map[string]string{}, nil
	}

	flat := make(map[string]string)
	if err := toml.Unmarshal(data, &flat); err == nil && isLegacyFlat(flat) {
		return flat, nil
	}

	var root map[string]interface{}
	if err := toml.Unmarshal(data, &root); err != nil {
		return nil, err
	}

	kvs := make(map[string]string)
	collectItems(root, nil, kvs)
	return kvs, nil
}

// isLegacyFlat reports whether every value looks like a JSON ConfigItem object.
func isLegacyFlat(flat map[string]string) bool {
	if len(flat) == 0 {
		return false
	}
	for _, v := range flat {
		if !strings.HasPrefix(strings.TrimSpace(v), "{") {
			return false
		}
	}
	return true
}

func collectItems(node map[string]interface{}, path []string, kvs map[string]string) {
	for key, val := range node {
		child, ok := val.(map[string]interface{})
		if !ok {
			continue
		}
		if isConfigLeaf(child) {
			kvs[strings.Join(append(path, key), "/")] = leafItem(child).String()
			continue
		}
		// Copy path for recursion: append reuses backing array across iterations.
		next := append(append([]string{}, path...), key)
		collectItems(child, next, kvs)
	}
}

func isConfigLeaf(node map[string]interface{}) bool {
	_, hasType := getField(node, "type", "Type")
	_, hasValue := getField(node, "value", "Value")
	return hasType && hasValue
}

func leafItem(node map[string]interface{}) substratum.ConfigItem {
	return substratum.ConfigItem{
		Type:    getString(node, "type", "Type"),
		Schema:  getString(node, "schema", "Schema"),
		Value:   getString(node, "value", "Value"),
		Comment: getString(node, "comment", "Comment"),
	}
}

func getField(node map[string]interface{}, keys ...string) (interface{}, bool) {
	for _, key := range keys {
		if v, ok := node[key]; ok && v != nil {
			return v, true
		}
	}
	return nil, false
}

func getString(node map[string]interface{}, keys ...string) string {
	if v, ok := getField(node, keys...); ok {
		return toString(v)
	}
	return ""
}

func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	// TOML may decode numbers/bools as non-string types.
	return fmt.Sprint(v)
}

// encodeKVs writes hierarchical TOML from flat path→ConfigItemJSON map.
func encodeKVs(kvs map[string]string) ([]byte, error) {
	root := make(map[string]interface{})
	keys := make([]string, 0, len(kvs))
	for key := range kvs {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		item, err := parseItemJSON(kvs[key])
		if err != nil {
			return nil, err
		}
		insertItem(root, strings.Split(key, "/"), item)
	}
	return toml.Marshal(root)
}

func insertItem(root map[string]interface{}, parts []string, item substratum.ConfigItem) {
	if len(parts) == 0 {
		return
	}
	if len(parts) == 1 {
		root[parts[0]] = itemFields(item)
		return
	}

	child, ok := root[parts[0]]
	if !ok {
		child = make(map[string]interface{})
		root[parts[0]] = child
	}
	insertItem(child.(map[string]interface{}), parts[1:], item)
}

func itemFields(item substratum.ConfigItem) map[string]string {
	return map[string]string{
		"type":    item.Type,
		"schema":  item.Schema,
		"value":   item.Value,
		"comment": item.Comment,
	}
}
