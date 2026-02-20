package redis

import "core:container/intrusive/list"
import "core:container/lru"
import "core:fmt"
import "core:time"

Database :: struct {
	cache: ^lru.Cache(string, Cachable),
}

Cachable :: union {
	String_Cachable,
	List_Cachable,
}

String_Cachable :: struct {
	using node: list.Node,
	value:      string,
	expires_at: time.Time,
}

List_Cachable :: struct {
	count:    i64,
	elements: list.List,
}

database_init :: proc(capacity: int = 1000, allocator := context.allocator) -> Database {
	db := Database{}
	db.cache = new(lru.Cache(string, Cachable), allocator)
	lru.init(db.cache, capacity, allocator)
	return db
}

database_destroy :: proc(db: ^Database) {
	lru.destroy(db.cache, true)
	free(db)
}

database_set :: proc(db: ^Database, key: string, value: Cachable) -> (ok: bool) {
	assert(db.cache.capacity > 0)

	err := lru.set(db.cache, key, value)
	if err != nil {
		fmt.printfln("Failed to set %s for %s: %s", value, key, err)
	}
	return err == nil
}

database_peek :: proc(db: ^Database, key: string) -> (value: Cachable, ok: bool) {
	return lru.peek(db.cache, key)
}

database_get :: proc(db: ^Database, key: string) -> (value: Cachable, ok: bool) {
	return lru.get(db.cache, key)
}

database_remove :: proc(db: ^Database, key: string) -> (ok: bool) {
	return lru.remove(db.cache, key)
}
