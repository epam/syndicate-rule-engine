package sre

import "encoding/json"

func decodeList[T any](raw json.RawMessage) ([]T, error) {
	var wrap struct {
		Items []T `json:"items"`
	}
	firstErr := json.Unmarshal(raw, &wrap)
	if firstErr == nil && len(wrap.Items) > 0 {
		return wrap.Items, nil
	}

	var dataWrap struct {
		Data struct {
			Items   []T `json:"items"`
			Content []T `json:"content"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &dataWrap); err != nil {
		if firstErr == nil {
			return wrap.Items, nil
		}
		return nil, err
	}
	if len(dataWrap.Data.Items) > 0 {
		return dataWrap.Data.Items, nil
	}
	return dataWrap.Data.Content, nil
}

func decodeMapContent[T any](content json.RawMessage) (map[string]T, error) {
	if len(content) == 0 || string(content) == "null" {
		return map[string]T{}, nil
	}
	var out map[string]T
	if err := json.Unmarshal(content, &out); err != nil {
		return nil, err
	}
	return out, nil
}
