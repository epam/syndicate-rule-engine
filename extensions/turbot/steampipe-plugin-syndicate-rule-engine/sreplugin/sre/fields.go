package sre

// StringField reads a string value from resource data map.
func StringField(data map[string]interface{}, key string) string {
	if data == nil {
		return ""
	}
	v, ok := data[key].(string)
	if ok {
		return v
	}
	return ""
}

// ResourceFields extracts common K8s resource identifiers from data.
func ResourceFields(data map[string]interface{}) (id, name, namespace, kind string) {
	return StringField(data, "id"),
		StringField(data, "name"),
		StringField(data, "namespace"),
		StringField(data, "kind")
}
