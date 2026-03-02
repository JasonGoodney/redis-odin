package redis

import linked_list "core:container/intrusive/list"
import "core:fmt"
import "core:math"
import "core:strconv"
import "core:strings"
import "core:sync"
import "core:time"

Redis_Error :: union #shared_nil {
	Database_Error,
	Stream_Error,
}

Stream_Error :: enum {
	None = 0,
	Invalid_Args,
	ID_Invalid_Form,
	ID_Seq_Zero,
	ID_Seq_Below_Top,
}

Database_Error :: enum {
	None = 0,
	Key_Not_Found,
	Mismatch_Type,
	Expired,
}

Redis_Value :: union {
	String,
	List,
	Stream,
}

// ====== Strings ===========================

String :: struct {
	expires_at: time.Time,
	value:      string,
}

string_set :: proc(db: ^Database, key: string, value: String) -> (error: Database_Error) {
	impl := database_lock(db)
	defer database_unlock(impl)
	return database_set(impl, key, value)
}

string_get :: proc(db: ^Database, key: string) -> (value: String, error: Database_Error) {
	impl := database_lock(db)
	defer database_unlock(impl)
	value = database_typed_get(impl, key, String) or_return
	return value, .None
}

// ====== Generic ===========================

generic_type :: proc(db: ^Database, key: string) -> (type: string, error: Database_Error) {
	impl := database_lock(db)
	defer database_unlock(impl)

	val := database_get(impl, key) or_return


	switch v in val {
	case String:
		type = "string"
	case List:
		type = "list"
	case Stream:
		type = "stream"
	}

	return type, .None
}

// ====== Lists ===========================

@(private)
List_Element :: struct {
	node:  linked_list.Node,
	value: string,
}

List :: struct {
	expires_at: time.Time,
	len:        i64,
	elements:   ^linked_list.List,
}

@(private)
list_init :: proc() -> List {
	l := List{}
	l.elements = new(linked_list.List)
	return l
}

list_length :: proc(db: ^Database, key: string) -> (length: i64, error: Database_Error) {
	impl := database_lock(db)
	defer database_unlock(impl)

	l := database_typed_get(impl, key, List) or_return
	return l.len, .None
}

list_get :: proc(db: ^Database, key: string) -> (list: List, error: Database_Error) {
	impl := database_lock(db)
	defer database_unlock(impl)
	l := database_typed_get(impl, key, List) or_return
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
	l := database_typed_get(impl, key, List) or_return

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

	iter := linked_list.iterator_head(l.elements^, List_Element, "node")
	i: i64 = 0
	for elem in linked_list.iterate_next(&iter) {
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

	l, get_err := database_typed_get(impl, key, List)
	if get_err != nil {
		l = list_init()
	}

	for val in values {
		elem := new(List_Element)
		elem.value = val
		linked_list.push_back(l.elements, &elem.node)
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

	l, get_err := database_typed_get(impl, key, List)
	if get_err != nil {
		l = list_init()
	}

	for val in values {
		elem := new(List_Element)
		elem.value = val
		linked_list.push_front(l.elements, &elem.node)
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

	l := database_typed_get(impl, key, List) or_return

	popped := make([dynamic]string)
	iter := linked_list.iterator_head(l.elements^, List_Element, "node")
	for i := 0; i < count; i += 1 {
		item, ok := linked_list.iterate_next(&iter)
		if !ok {
			break
		}
		append(&popped, item.value)
		linked_list.pop_front(l.elements)
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

	l := database_typed_get(impl, key, List) or_return

	popped := make([dynamic]string)
	iter := linked_list.iterator_tail(l.elements^, List_Element, "node")
	for i := 0; i < count; i += 1 {
		item, ok := linked_list.iterate_prev(&iter)
		if !ok {
			break
		}
		append(&popped, item.value)
		linked_list.pop_back(l.elements)
	}

	database_set(impl, key, l)

	return popped[:], .None
}

// ====== Stream ===========================

Stream_ID :: struct {
	ms:  u64,
	seq: u64,
}

Stream_ID_Zero :: Stream_ID{}
Stream_ID_Min :: Stream_ID{0, 1}
Stream_ID_Max :: Stream_ID{max(u64), max(u64)}

Stream_Entry :: struct {
	using node: linked_list.Node,
	id:         Stream_ID,
	fields:     map[string]string,
}

Stream :: struct {
	expires_at: time.Time,
	len:        i64,
	elements:   ^linked_list.List,
}

@(private)
stream_init :: proc() -> Stream {
	s := Stream{}
	s.elements = new(linked_list.List)
	return s
}

@(private)
stream_entry_init :: proc() -> ^Stream_Entry {
	s := new(Stream_Entry)
	s.fields = make(type_of(s.fields))
	return s
}

stream_add :: proc(
	db: ^Database,
	key: string,
	id: string,
	fields: map[string]string,
) -> (
	entry: ^Stream_Entry,
	err: Redis_Error,
) {
	impl := database_lock(db)
	defer database_unlock(impl)

	stream, get_err := database_typed_get(impl, key, Stream)
	if get_err == .Key_Not_Found {
		stream = stream_init()
	} else if get_err != nil {
		return {}, err
	}

	iter := linked_list.iterator_tail(stream.elements^, Stream_Entry, "node")
	last_entry, iter_ok := linked_list.iterate_prev(&iter)
	last_entry_id: Stream_ID
	if iter_ok {
		last_entry_id = last_entry.id
	}

	ms, seq := _parse_stream_id(id, last_entry_id.ms, last_entry_id.seq) or_return
	entry = stream_entry_init()
	entry.id = {ms, seq}
	entry.fields = fields

	linked_list.push_back(stream.elements, &entry.node)
	stream.len += 1

	database_set(impl, key, stream) or_return

	return entry, Stream_Error.None
}

stream_range :: proc(
	db: ^Database,
	key: string,
	start: string,
	end: string,
) -> (
	entries: []Stream_Entry,
	err: Stream_Error,
) {
	impl := database_lock(db)
	defer database_unlock(impl)

	stream, get_err := database_typed_get(impl, key, Stream)
	if get_err == .Key_Not_Found {
		stream = stream_init()
	} else if get_err != nil {
		return {}, err
	}

	start_id, start_ok := _stream_range_parse_id(start, 0)
	end_id, end_ok := _stream_range_parse_id(end, max(u64))
	if !start_ok || !end_ok {
		return {}, .ID_Invalid_Form
	}

	_entries := make([dynamic]Stream_Entry)

	iter := linked_list.iterator_head(stream.elements^, Stream_Entry, "node")
	for elem in linked_list.iterate_next(&iter) {
		if .Greater == _stream_id_compare(start_id, elem.id) {
			continue
		}
		if .Less == _stream_id_compare(end_id, elem.id) {
			break
		}

		append(&_entries, elem^)
	}
	return _entries[:], .None
}

stream_read :: proc(
	db: ^Database,
	key: string,
	id: string,
) -> (
	entries: []Stream_Entry,
	err: Stream_Error,
) {
	impl := database_lock(db)
	defer database_unlock(impl)

	stream, get_err := database_typed_get(impl, key, Stream)
	if get_err == .Key_Not_Found {
		stream = stream_init()
	} else if get_err != nil {
		return {}, err
	}

	start_id, start_ok := _stream_range_parse_id(id, 0)
	if !start_ok {
		return {}, .ID_Invalid_Form
	}

	_entries := make([dynamic]Stream_Entry)

	iter := linked_list.iterator_head(stream.elements^, Stream_Entry, "node")
	for elem in linked_list.iterate_next(&iter) {
		lhs := _stream_id_compare(elem.id, start_id)
		if lhs == .Greater {
			append(&_entries, elem^)
		}
	}
	return _entries[:], .None
}

stream_top :: proc(db: ^Database, key: string) -> (entry: ^Stream_Entry, err: Stream_Error) {

	impl := database_lock(db)
	defer database_unlock(impl)

	stream, get_err := database_typed_get(impl, key, Stream)
	if get_err == .Key_Not_Found {
		stream = stream_init()
	} else if get_err != nil {
		return {}, err
	}

	iter := linked_list.iterator_tail(stream.elements^, Stream_Entry, "node")
	elem, _ := linked_list.iterate_prev(&iter)
	return elem, .None
}

Compare_Op :: enum {
	Equal,
	Less,
	Greater,
}

// Returns how the lhs compares to rhs.
_stream_id_compare :: proc(lhs: Stream_ID, rhs: Stream_ID) -> Compare_Op {
	if lhs.ms == rhs.ms {
		if lhs.seq == rhs.seq {
			return .Equal
		} else if lhs.seq > rhs.seq {
			return .Greater
		} else {
			return .Less
		}
	} else if lhs.ms > rhs.ms {
		return .Greater
	} else {
		return .Less
	}
}

_stream_range_parse_id :: proc(id_arg: string, default_seq: u64) -> (id: Stream_ID, ok: bool) {
	if 0 == strings.compare(id_arg, "-") {
		return Stream_ID_Min, true
	} else if 0 == strings.compare(id_arg, "+") {
		return Stream_ID_Max, true
	}

	parts := strings.split(id_arg, "-")
	_ms, ms_ok := strconv.parse_u64(parts[0])

	if !ms_ok {
		return Stream_ID_Zero, false
	}
	if len(parts) == 1 {
		return {_ms, default_seq}, true
	}
	_seq, seq_ok := strconv.parse_u64(parts[1])
	if !seq_ok {
		return Stream_ID_Zero, false
	}
	return {_ms, _seq}, true
}

_parse_stream_id :: proc(
	id_arg: string,
	prev_ms: u64,
	prev_seq: u64,
) -> (
	ms: u64,
	seq: u64,
	err: Stream_Error,
) {
	if 0 == strings.compare(id_arg, "*") {
		return _parse_stream_id_auto_gen(prev_ms, prev_seq)
	}

	parts := strings.split(id_arg, "-")
	defer delete(parts)
	if len(parts) != 2 {
		return 0, 0, .ID_Invalid_Form
	}

	_ms, ms_ok := strconv.parse_u64(parts[0])
	_seq, seq_ok := strconv.parse_u64(parts[1])

	if !ms_ok {
		return 0, 0, .ID_Invalid_Form
	}

	if !seq_ok {
		if 0 == strings.compare(parts[1], "*") {
			return _parse_stream_id_seq_auto_gen(_ms, prev_ms, prev_seq)
		} else {
			return 0, 0, .ID_Invalid_Form
		}
	}

	return _parse_stream_id_explicit(_ms, _seq, prev_ms, prev_seq)
}

_parse_stream_id_auto_gen :: proc(prev_ms: u64, prev_seq: u64) -> (u64, u64, Stream_Error) {
	now := time.now()
	ns := time.to_unix_nanoseconds(now)
	dur := time.Duration(ns)
	msf := time.duration_milliseconds(dur)
	ms := u64(msf)

	return _parse_stream_id_seq_auto_gen(ms, prev_ms, prev_seq)
}
_parse_stream_id_seq_auto_gen :: proc(
	ms: u64,
	prev_ms: u64,
	prev_seq: u64,
) -> (
	u64,
	u64,
	Stream_Error,
) {
	if ms > prev_ms {
		return ms, 0, .None
	}
	return _parse_stream_id_explicit(ms, prev_seq + 1, prev_ms, prev_seq)
}
_parse_stream_id_explicit :: proc(
	ms: u64,
	seq: u64,
	prev_ms: u64,
	prev_seq: u64,
) -> (
	u64,
	u64,
	Stream_Error,
) {
	if ms == 0 && seq == 0 {
		return 0, 0, .ID_Seq_Zero
	}
	if ms > prev_ms {
		return ms, seq, .None
	} else if ms == prev_ms {
		if seq <= prev_seq {
			return 0, 0, .ID_Seq_Below_Top
		}
		return ms, seq, .None
	} else {
		return 0, 0, .ID_Seq_Below_Top
	}
}

stream_entry_id_string :: proc(id: Stream_ID) -> string {
	return fmt.tprintf("%d-%d", id.ms, id.seq)
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
database_set :: proc(
	impl: ^Database_Impl,
	key: string,
	value: Redis_Value,
) -> (
	error: Database_Error,
) {
	assert(impl.is_locked)
	impl.db[key] = value

	return .None
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
		case String:
			expires_at = v.expires_at
		case List:
			expires_at = v.expires_at
		case Stream:
			expires_at = v.expires_at
		}
		if expires_at != {} && time.diff(time.now(), expires_at) < 0 {
			fmt.printfln("Item expired for %s", key)
			delete_key(&impl.db, key)
			return {}, .Expired
		}
		return val, .None
	}

	return {}, .Key_Not_Found
}

error_string :: proc(err: Redis_Error) -> string {
	if err == nil {
		return ""
	}

	switch e in err {
	case Stream_Error:
		switch e {
		case .None:
			return ""
		case .Invalid_Args:
			return ""
		case .ID_Invalid_Form:
			return "ERR The ID specified in XADD must be in the form <ms-seq>"
		case .ID_Seq_Zero:
			return "ERR The ID specified in XADD must be greater than 0-0"
		case .ID_Seq_Below_Top:
			return(
				"ERR The ID specified in XADD is equal or smaller than the target stream top item" \
			)
		}
	case Database_Error:
		switch e {
		case .None:
			return ""
		case .Key_Not_Found:
			return "ERR Key not found"
		case .Mismatch_Type:
			return ""
		case .Expired:
			return "ERR Key expired"
		}
	}

	return "unknown error"
}

