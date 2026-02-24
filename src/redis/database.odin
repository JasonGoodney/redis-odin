package redis

import "core:container/intrusive/list"
import "core:sync"
import "core:time"

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

Database_Impl :: struct {
	database: Database,
	kv:       map[string]Redis_Value,
	lock:     sync.RW_Mutex,
}

Database :: struct {
	impl: rawptr,
}

@(private)
database_impl :: proc(database: ^Database) -> ^Database_Impl {
	assert(database != nil && database.impl != nil, "Invalid pointer")
	return (^Database_Impl)(database.impl)
}

database_init :: proc(capacity: int = 100, allocator := context.allocator) -> ^Database {
	impl := new(Database_Impl, allocator)
	impl.database.impl = impl
	impl.kv = make(map[string]Redis_Value, allocator)

	return &impl.database
}

database_destroy :: proc(database: ^Database) {
	db := database_impl(database)
	delete(db.kv)
	free(db)
}

database_set :: proc(database: ^Database, key: string, value: Redis_Value) -> (ok: bool) {
	db := database_impl(database)

	sync.rw_mutex_lock(&db.lock)
	db.kv[key] = value
	sync.rw_mutex_unlock(&db.lock)

	return true
}

database_list_pop :: proc(
	database: ^Database,
	key: string,
	list: ^List_Value,
	count: int = 1,
	popper: proc(l: ^List_Value, count: int = 1) -> []string,
) -> (
	popped: []string,
	ok: bool,
) {
	db := database_impl(database)

	sync.rw_mutex_lock(&db.lock)
	popped = popper(list, count)
	db.kv[key] = list^
	sync.rw_mutex_unlock(&db.lock)

	return popped, true
}

database_get_type :: proc(database: ^Database, key: string, $T: typeid) -> (value: T, ok: bool) {
	val, get_ok := database_get(database, key)

	if get_ok {
		v, cast_ok := val.(T)
		if cast_ok {
			return v, true
		}
	}

	return {}, false
}

database_get :: proc(database: ^Database, key: string) -> (value: Redis_Value, ok: bool) {
	db := database_impl(database)
	sync.rw_mutex_lock(&db.lock)
	val, get_ok := db.kv[key]
	sync.rw_mutex_unlock(&db.lock)

	if get_ok {
		return val, true
	}

	return {}, false
}

database_remove :: proc(database: ^Database, key: string) -> (ok: bool) {
	db := database_impl(database)
	sync.rw_mutex_lock(&db.lock)
	delete_key(&db.kv, key)
	sync.rw_mutex_unlock(&db.lock)

	return ok
}

