package redis

import "core:container/intrusive/list"
import "core:container/lru"
import "core:fmt"
import "core:sync"
import "core:time"

Database :: struct {
	cache: ^lru.Cache(string, Redis_Value),
	lock:  sync.RW_Mutex,
}

Redis_Value :: union {
	String_Value,
	List_Value,
}

String_Value :: struct {
	value:      string,
	expires_at: time.Time,
}

@(private)
List_Element :: struct {
	node:  list.Node,
	value: string,
}

List_Value :: struct {
	len:      int,
	elements: ^list.List,
}

list_init :: proc() -> List_Value {
	l := List_Value{}
	l.elements = new(list.List)
	return l
}

list_append :: proc(l: ^List_Value, value: string) {
	item := new(List_Element)
	item.value = value
	list.push_back(l.elements, &item.node)
	l.len += 1
}

list_prepend :: proc(l: ^List_Value, value: string) {
	item := new(List_Element)
	item.value = value
	list.push_front(l.elements, &item.node)
	l.len += 1
}

list_pop_front :: proc(l: ^List_Value, count: int = 1) -> []string {
	popped := make([dynamic]string)

	iter := list.iterator_head(l.elements^, List_Element, "node")
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

list_pop_back :: proc(l: ^List_Value, count: int = 1) -> []string {
	popped := make([dynamic]string)

	iter := list.iterator_tail(l.elements^, List_Element, "node")
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
	db.cache = new(lru.Cache(string, Redis_Value), allocator)
	lru.init(db.cache, capacity, allocator)
	return db
}

database_destroy :: proc(db: ^Database) {
	lru.destroy(db.cache, true)
	free(db)
}

database_set :: proc(db: ^Database, key: string, value: Redis_Value) -> (ok: bool) {
	assert(db.cache.capacity > 0)

	sync.rw_mutex_lock(&db.lock)
	err := lru.set(db.cache, key, value)
	sync.rw_mutex_unlock(&db.lock)

	if err != nil {
		fmt.printfln("Failed to set %s for %s: %s", value, key, err)
	}
	return err == nil
}

database_peek :: proc(db: ^Database, key: string) -> (value: Redis_Value, ok: bool) {
	sync.rw_mutex_lock(&db.lock)
	value, ok = lru.peek(db.cache, key)
	sync.rw_mutex_unlock(&db.lock)

	return value, ok
}

database_get :: proc(db: ^Database, key: string) -> (value: Redis_Value, ok: bool) {
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
