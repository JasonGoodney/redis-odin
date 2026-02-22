package redis

import "core:container/intrusive/list"
import "core:container/lru"
import "core:fmt"
import "core:sync"
import "core:time"

Database :: struct {
	cache: ^lru.Cache(string, Cachable),
	lock:  sync.RW_Mutex,
}

Cachable :: union {
	String_Cachable,
	List_Cachable,
}

String_Cachable :: struct {
	value:      string,
	expires_at: time.Time,
}

List_Cachable :: struct {
	elements: [dynamic]string,
}

database_init :: proc(capacity: int = 100, allocator := context.allocator) -> Database {
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

	sync.rw_mutex_lock(&db.lock)
	err := lru.set(db.cache, key, value)
	sync.rw_mutex_unlock(&db.lock)

	if err != nil {
		fmt.printfln("Failed to set %s for %s: %s", value, key, err)
	}
	return err == nil
}

database_peek :: proc(db: ^Database, key: string) -> (value: Cachable, ok: bool) {
	sync.rw_mutex_lock(&db.lock)
	value, ok = lru.peek(db.cache, key)
	sync.rw_mutex_unlock(&db.lock)

	return value, ok
}

database_get :: proc(db: ^Database, key: string) -> (value: Cachable, ok: bool) {
	sync.rw_mutex_lock(&db.lock)
	value, ok = lru.get(db.cache, key)
	sync.rw_mutex_unlock(&db.lock)

	return value, ok
}

database_remove :: proc(db: ^Database, key: string) -> (ok: bool) {
	sync.rw_mutex_lock(&db.lock)
	ok = lru.remove(db.cache, key)
	sync.rw_mutex_unlock(&db.lock)

	return ok
}
