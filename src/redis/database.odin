package redis

import "core:container/intrusive/list"
import "core:math"
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

string_set :: proc(db: ^Database, key: string, value: String_Value) -> (ok: bool) {
	impl := database_lock(db)
	defer database_unlock(impl)
	return database_set(impl, key, value)
}

string_get :: proc(db: ^Database, key: string) -> (value: String_Value, error: Database_Error) {
	impl := database_lock(db)
	defer database_unlock(impl)
	value = database_typed_get(impl, key, String_Value) or_return
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

@(private)
list_init :: proc() -> List_Value {
	l := List_Value{}
	l.elements = new(list.List)
	return l
}

list_length :: proc(db: ^Database, key: string) -> (length: i64, error: Database_Error) {
	impl := database_lock(db)
	defer database_unlock(impl)

	l := database_typed_get(impl, key, List_Value) or_return
	return l.len, .None
}

list_get :: proc(db: ^Database, key: string) -> (list: List_Value, error: Database_Error) {
	impl := database_lock(db)
	defer database_unlock(impl)
	l := database_typed_get(impl, key, List_Value) or_return
	return l, .None
}

list_range :: proc(
	db: ^Database,
	key: string,
	start: i64,
	stop: i64,
) -> (
	values: []string,
	error: Database_Error,
) {
	impl := database_lock(db)
	defer database_unlock(impl)
	l := database_typed_get(impl, key, List_Value) or_return

	elem_count := l.len

	start := start
	stop := stop
	if start > elem_count {
		return {}, .None
	}
	if math.abs(start) > elem_count {
		start = 0
	}
	if start < 0 {
		start = elem_count + start
	}

	if stop > elem_count {
		stop = elem_count - 1
	}
	if math.abs(stop) > elem_count {
		stop = 0
	}
	if stop < 0 {
		stop = elem_count + stop
	}

	vals := make([dynamic]string)

	iter := list.iterator_head(l.elements^, List_Element, "node")
	i: i64 = 0
	for elem in list.iterate_next(&iter) {
		if elem.value == {} {
			break
		}
		if i < start {
			i += 1
			continue
		}
		if i > stop {
			break
		}
		append(&vals, elem.value)
		i += 1
	}

	return vals[:], .None

}

list_push_back :: proc(
	db: ^Database,
	key: string,
	values: ..string,
) -> (
	count: i64,
	error: Database_Error,
) {
	impl := database_lock(db)
	defer database_unlock(impl)

	l, get_err := database_typed_get(impl, key, List_Value)
	if get_err != nil {
		l = list_init()
	}

	for val in values {
		elem := new(List_Element)
		elem.value = val
		list.push_back(l.elements, &elem.node)
		l.len += 1
	}
	database_set(impl, key, l)
	return l.len, .None
}

list_push_front :: proc(
	db: ^Database,
	key: string,
	values: ..string,
) -> (
	count: i64,
	error: Database_Error,
) {
	impl := database_lock(db)
	defer database_unlock(impl)

	l, get_err := database_typed_get(impl, key, List_Value)
	if get_err != nil {
		l = list_init()
	}

	for val in values {
		elem := new(List_Element)
		elem.value = val
		list.push_front(l.elements, &elem.node)
		l.len += 1
	}
	database_set(impl, key, l)
	return l.len, .None
}

list_pop_front :: proc(
	db: ^Database,
	key: string,
	count: int,
) -> (
	values: []string,
	error: Database_Error,
) {
	impl := database_lock(db)
	defer database_unlock(impl)

	l := database_typed_get(impl, key, List_Value) or_return

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

	database_set(impl, key, l)

	return popped[:], .None
}

list_pop_back :: proc(
	db: ^Database,
	key: string,
	count: int,
) -> (
	values: []string,
	error: Database_Error,
) {
	impl := database_lock(db)
	defer database_unlock(impl)

	l := database_typed_get(impl, key, List_Value) or_return

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

	database_set(impl, key, l)

	return popped[:], .None
}

// ====== Generic ===========================

generic_type :: proc(db: ^Database, key: string) -> (type: string, error: Database_Error) {
	impl := database_lock(db)
	defer database_unlock(impl)

	val := database_get(impl, key) or_return


	switch v in val {
	case String_Value:
		type = "string"
	case List_Value:
		type = "list"
	}

	return type, .None
}

// ====== Database ===========================

@(private)
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

database_init :: proc(capacity: int = 100, allocator := context.allocator) -> ^Database {
	impl := new(Database_Impl, allocator)
	impl.handle.impl = impl
	impl.db = make(map[string]Redis_Value, allocator)

	return &impl.handle
}

@(private)
database_impl :: proc(database: ^Database) -> ^Database_Impl {
	assert(database != nil && database.impl != nil, "Invalid database pointer")
	return (^Database_Impl)(database.impl)
}

@(private)
database_lock :: proc(db: ^Database) -> ^Database_Impl {
	impl := database_impl(db)
	sync.rw_mutex_lock(&impl.lock)
	impl.is_locked = true
	return impl
}

@(private)
database_unlock :: proc(impl: ^Database_Impl) {
	sync.rw_mutex_unlock(&impl.lock)
	impl.is_locked = false
}


@(private)
database_destroy :: proc(database: ^Database) {
	db := database_impl(database)
	delete(db.db)
	free(db)
}

@(private)
database_set :: proc(impl: ^Database_Impl, key: string, value: Redis_Value) -> (ok: bool) {
	assert(impl.is_locked)
	impl.db[key] = value

	return true
}

@(private)
database_typed_get :: proc(
	impl: ^Database_Impl,
	key: string,
	$T: typeid,
) -> (
	value: T,
	error: Database_Error,
) {
	val := database_get(impl, key) or_return

	v, cast_ok := val.(T)
	if !cast_ok {
		return {}, .Mismatch_Type
	}

	return v, .None
}

@(private)
database_get :: proc(
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

