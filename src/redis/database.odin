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
	List,
}

String_Cachable :: struct {
	value:      string,
	expires_at: time.Time,
}

List_Item :: struct {
	node:  list.Node,
	value: string,
}

List :: struct {
	len:      int,
	elements: ^list.List,
}

list_init :: proc() -> List {
	l := List{}
	l.elements = new(list.List)
	return l
}

list_append :: proc(l: ^List, value: string) {
	item := new(List_Item)
	item.value = value
	list.push_back(l.elements, &item.node)
	l.len += 1
}

list_prepend :: proc(l: ^List, value: string) {
	item := new(List_Item)
	item.value = value
	list.push_front(l.elements, &item.node)
	l.len += 1
}

list_pop_front :: proc(l: ^List, count: int = 1) -> []string {
	popped := make([dynamic]string)

	iter := list.iterator_head(l.elements^, List_Item, "node")
	for i := 0; i < count; i += 1 {
		item, ok := list.iterate_next(&iter)
		if !ok {
			break
		}
		append(&popped, item.value)
		list.pop_front(l.elements)
	}

	return popped[:]
}

list_pop_back :: proc(l: ^List, count: int = 1) -> []string {
	popped := make([dynamic]string)

	iter := list.iterator_tail(l.elements^, List_Item, "node")
	for i := 0; i < count; i += 1 {
		item, ok := list.iterate_prev(&iter)
		if !ok {
			break
		}
		append(&popped, item.value)
		list.pop_back(l.elements)
	}

	return popped[:]
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
