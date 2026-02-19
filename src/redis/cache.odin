package redis

import "core:container/lru"
import "core:fmt"
import "core:strconv"
import "core:time"

Set_Op_Data :: struct {
	value:         string,
	expirable:     bool,
	expires_at_ms: i64,
}

cache := lru.Cache(string, Set_Op_Data){}

cache_init :: proc(capacity: int = 1000) {
	lru.init(&cache, capacity)
}

cache_destroy :: proc() {
	lru.destroy(&cache, true)
}

cache_set :: proc(key: string, value: string, options: []Set_Option) -> (ok: bool) {
	assert(cache.capacity > 0)

	data := Set_Op_Data {
		value = value,
	}
	for opt in options {
		switch o in opt {
		case Set_Option_PX:
			now_ms := time.now()._nsec / 1_000_000
			data.expires_at_ms = now_ms + o.expire_milliseconds
			data.expirable = true
		}
	}
	fmt.printfln("Setting %s for %s", value, key)
	err := lru.set(&cache, key, data)
	if err != nil {
		fmt.printfln("Failed to set %s for %s: %s", value, key, err)
	}
	return err == nil
}

cache_get :: proc(key: string) -> (value: string, ok: bool) {
	data, get_ok := lru.get(&cache, key)
	if !get_ok {
		fmt.printfln("Item not found for %s", key)
		return "", false
	}

	if data.expirable && data.expires_at_ms < time.now()._nsec / 1_000_000 {
		fmt.printfln("Item expired for %s", key)
		lru.remove(&cache, key)
		return "", false
	}

	return data.value, true
}
