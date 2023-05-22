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
