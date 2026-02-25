package redis

import "base:intrinsics"
import "core:container/intrusive/list"
import "core:sync"
import "core:time"

Redis_Value :: union {
	String_Value,
	List_Value,
}

// ====== Strings ===========================

String_Value :: struct {
	expires_at: time.Time,
	value:      string,
}

strings_set :: proc(db: ^Database, key: string, value: String_Value) -> (ok: bool) {
	impl := database_lock(db)
	defer database_unlock(impl)
	return _database_set(impl, key, value)
}

strings_get :: proc(db: ^Database, key: string) -> (value: String_Value, error: Database_Error) {
	impl := database_lock(db)
	defer database_unlock(impl)
	value = _database_get_type(impl, key, String_Value) or_return
	return value, .None
}

// ====== Lists ===========================

@(private)
List_Element :: struct {
	node:  list.Node,
	value: string,
}

List_Value :: struct {
	expires_at: time.Time,
	len:        i64,
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

list_push_back :: proc(
	db: ^Database,
	key: string,
	values: ..string,
) -> (
	count: i64,
	error: Database_Error,
) {
	impl := database_impl(db)
	l, get_err := database_get_type(db, key, List_Value)
	if get_err != nil {
		return 0, get_err
	}

	for val in values {
		elem := new(List_Element)
		list.push_back(l.elements, &elem.node)
		l.len += 1
	}
	database_set(db, key, l)
	return l.len, .None
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

// ====== Database ===========================

Database_Impl :: struct {
	handle:    Database,
	db:        map[string]Redis_Value,
	lock:      sync.RW_Mutex,
	is_locked: bool,
}

Database :: struct {
	impl: rawptr,
}

Database_Error :: enum {
	None = 0,
	Key_Not_Found,
	Mismatch_Type,
	Expired,
}

@(private)
database_impl :: proc(database: ^Database) -> ^Database_Impl {
	assert(database != nil && database.impl != nil, "Invalid database pointer")
	return (^Database_Impl)(database.impl)
}

database_lock :: proc(db: ^Database) -> ^Database_Impl {
	impl := database_impl(db)
	sync.rw_mutex_lock(&impl.lock)
	impl.is_locked = true
	return impl
}

database_unlock :: proc(impl: ^Database_Impl) {
	sync.rw_mutex_unlock(&impl.lock)
	impl.is_locked = false
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

	return {}, .Key_Not_Found
}

_database_set :: proc(impl: ^Database_Impl, key: string, value: Redis_Value) -> (ok: bool) {
	assert(impl.is_locked)
	impl.db[key] = value

	return true
}

_database_get_type :: proc(
	impl: ^Database_Impl,
	key: string,
	$T: typeid,
) -> (
	value: T,
	error: Database_Error,
) {
	val := _database_get(impl, key) or_return

	v, cast_ok := val.(T)
	if !cast_ok {
		return {}, .Mismatch_Type
	}

	return v, .None
}

_database_get :: proc(
	impl: ^Database_Impl,
	key: string,
) -> (
	value: Redis_Value,
	error: Database_Error,
) {
	assert(impl.is_locked)
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

	return {}, .Key_Not_Found
}

