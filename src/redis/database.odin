package redis

import "core:container/intrusive/list"
import "core:sync"
import "core:time"

Redis_Value :: union {
	String_Value,
	List_Value,
}

String_Value :: struct {
	expires_at: time.Time,
	value:      string,
}

@(private)
List_Element :: struct {
	node:  list.Node,
	value: string,
}

List_Value :: struct {
	expires_at: time.Time,
	len:        int,
	elements:   ^list.List,
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
	handle: Database,
	db:     map[string]Redis_Value,
	lock:   sync.RW_Mutex,
}

Database :: struct {
	impl: rawptr,
}

Database_Error :: enum {
	None = 0,
	Not_Found,
	Mismatch_Type,
	Expired,
}

@(private)
database_impl :: proc(database: ^Database) -> ^Database_Impl {
	assert(database != nil && database.impl != nil, "Invalid database pointer")
	return (^Database_Impl)(database.impl)
}

database_init :: proc(capacity: int = 100, allocator := context.allocator) -> ^Database {
	impl := new(Database_Impl, allocator)
	impl.handle.impl = impl
	impl.db = make(map[string]Redis_Value, allocator)

	return &impl.handle
}

database_destroy :: proc(database: ^Database) {
	db := database_impl(database)
	delete(db.db)
	free(db)
}

strings_set :: proc(db: ^Database, key: string, value: String_Value) -> (ok: bool) {
	return database_set(db, key, value)
}

strings_get :: proc(db: ^Database, key: string) -> (value: String_Value, error: Database_Error) {
	value = database_get_type(db, key, String_Value) or_return
	return value, .None
}

database_set :: proc(database: ^Database, key: string, value: Redis_Value) -> (ok: bool) {
	db := database_impl(database)

	sync.rw_mutex_lock(&db.lock)
	db.db[key] = value
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
	db.db[key] = list^
	sync.rw_mutex_unlock(&db.lock)

	return popped, true
}

database_get_type :: proc(
	database: ^Database,
	key: string,
	$T: typeid,
) -> (
	value: T,
	error: Database_Error,
) {
	val := database_get(database, key) or_return

	v, cast_ok := val.(T)
	if !cast_ok {
		return {}, .Mismatch_Type
	}

	return v, .None
}

database_get :: proc(db: ^Database, key: string) -> (value: Redis_Value, error: Database_Error) {
	impl := database_impl(db)
	sync.rw_mutex_lock(&impl.lock)
	defer sync.rw_mutex_unlock(&impl.lock)
	val, get_ok := impl.db[key]

	if get_ok {
		expires_at: time.Time
		switch v in val {
		case String_Value:
			expires_at = v.expires_at
		case List_Value:
			expires_at = v.expires_at
		}
		if expires_at != {} && time.diff(time.now(), expires_at) < 0 {
			// fmt.printfln("Item expired for %s", key)
			delete_key(&impl.db, key)
			return {}, .Expired
		}
		return val, .None
	}

	return {}, .Not_Found
}

