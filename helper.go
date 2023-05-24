package batcher

// enrichErrors enrich the result with errors
func enrichErrors[K comparable, V any](keys []K, data map[K]V, errors map[K]error, err error) {
	var ok bool

	for _, key := range keys {
		_, ok = data[key]
		if ok {
			continue
		}

		_, ok = errors[key]
		if ok {
			continue
		}

		errors[key] = err
	}
}

func scoringKey[K comparable](keys []K) []K {
	keymap := make(map[K]struct{}, len(keys))
	result := make([]K, 0, len(keys))

	var ok bool
	for _, key := range keys {
		_, ok = keymap[key]
		if !ok {
			keymap[key] = struct{}{}
			result = append(result, key)
		}
	}

	return result
}

// FailResult return slice of Result with error
func FailResult[K comparable, V any](keys []K, err error) []*Result[K, V] {
	result := make([]*Result[K, V], 0, len(keys))
	for _, key := range keys {
		result = append(result, &Result[K, V]{Key: key, Err: err})
	}

	return result
}
