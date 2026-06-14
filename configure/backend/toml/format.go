package toml

import (
	"encoding/json"
	"sort"
	"strings"

	substratum "github.com/appootb/substratum/v2/plugin/configure"
	toml "github.com/pelletier/go-toml"
)

func parseItemJSON(s string) (substratum.ConfigItem, error) {
	var item substratum.ConfigItem
	err := json.Unmarshal([]byte(s), &item)
	return item, err
}

func decodeKVs(data []byte) (map[string]string, error) {
	if len(data) == 0 {
		return map[string]string{}, nil
	}
	//
	flat := make(map[string]string)
	if err := toml.Unmarshal(data, &flat); err == nil && isLegacyFlat(flat) {
		return flat, nil
	}
	//
	tree, err := toml.LoadBytes(data)
	if err != nil {
		return nil, err
	}
	//
	kvs := make(map[string]string)
	collectItems(tree, nil, kvs)
	return kvs, nil
}

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

func collectItems(tree *toml.Tree, path []string, kvs map[string]string) {
	for _, key := range tree.Keys() {
		node := tree.Get(key)
		switch v := node.(type) {
		case *toml.Tree:
			if isConfigLeaf(v) {
				kvs[strings.Join(append(path, key), "/")] = leafItem(v).String()
				continue
			}
			collectItems(v, append(path, key), kvs)
		}
	}
}

func isConfigLeaf(tree *toml.Tree) bool {
	hasType := tree.Get("type") != nil || tree.Get("Type") != nil
	hasValue := tree.Get("value") != nil || tree.Get("Value") != nil
	return hasType && hasValue
}

func leafItem(tree *toml.Tree) substratum.ConfigItem {
	return substratum.ConfigItem{
		Type:    getString(tree, "type", "Type"),
		Schema:  getString(tree, "schema", "Schema"),
		Value:   getString(tree, "value", "Value"),
		Comment: getString(tree, "comment", "Comment"),
	}
}

func getString(tree *toml.Tree, keys ...string) string {
	for _, key := range keys {
		if v := tree.Get(key); v != nil {
			return toString(v)
		}
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
	return ""
}

func encodeKVs(kvs map[string]string) ([]byte, error) {
	root := make(map[string]interface{})
	keys := make([]string, 0, len(kvs))
	for key := range kvs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	//
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
	//
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
