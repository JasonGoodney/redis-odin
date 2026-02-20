package redis

import "core:bytes"
import "core:fmt"
import "core:strconv"
import "core:strings"

RESP_TERMINATOR_BUF :: []byte{'\r', '\n'}

RESP_Data_Type :: enum rune {
	Simple_String   = '+',
	Simple_Error    = '-',
	Integer         = ':',
	Bulk_String     = '$',
	Array           = '*',
	Null            = '_',
	Boolean         = '#',
	Double          = ',',
	Big_Number      = '(',
	Bulk_Error      = '!',
	Verbatim_String = '=',
	Map             = '%',
	Attribute       = '|',
	Set             = '~',
	Pushe           = '>',
}

RESP_Error :: enum {
	None,
	Unexpected_End,
	Unknown_Starting_Byte,
	IO_Error,
	Int_Parse_Failure,
	Bad_Bulk_String_Size,
	Bad_Array_Size,
}

Buf_Slice :: struct {
	start, end: int,
}

RESP :: union {
	RESP_Simple_String,
	RESP_Simple_Error,
	RESP_Integer,
	RESP_Bulk_String,
	RESP_Array,
	RESP_Null_Bulk_String,
	RESP_Null_Array,
}

RESP_Simple_String :: struct {
	value: string,
}

RESP_Integer :: struct {
	value: i64,
}

RESP_Array :: struct {
	elements: [dynamic]RESP,
}

RESP_Simple_Error :: distinct RESP_Simple_String
RESP_Bulk_String :: distinct RESP_Simple_String
RESP_Null_Bulk_String :: struct {}
RESP_Null_Array :: struct {}

decode :: proc(buf: []byte) -> (RESP, RESP_Error) {
	data, _, err := parse(buf, 0)
	if err != nil {
		return {}, err
	}
	return data, .None
}

encode :: proc(type: RESP) -> (str: string, ok: bool) #optional_ok {
	sb := strings.builder_make()
	defer strings.builder_destroy(&sb)

	switch t in type {
	case RESP_Simple_String:
		strings.write_rune(&sb, '+')
		strings.write_string(&sb, t.value)
		strings.write_string(&sb, "\r\n")
	case RESP_Simple_Error:
		strings.write_rune(&sb, '-')
		strings.write_string(&sb, t.value)
		strings.write_string(&sb, "\r\n")
	case RESP_Integer:
		strings.write_rune(&sb, ':')
		strings.write_i64(&sb, t.value)
		strings.write_string(&sb, "\r\n")
	case RESP_Bulk_String:
		strings.write_rune(&sb, '$')
		strings.write_int(&sb, len(t.value))
		strings.write_string(&sb, "\r\n")
		strings.write_string(&sb, t.value)
		strings.write_string(&sb, "\r\n")
	case RESP_Array:
		strings.write_rune(&sb, '*')
		strings.write_int(&sb, len(t.elements))
		strings.write_string(&sb, "\r\n")
		for e in t.elements {
			str, ok := encode(e)
			strings.write_string(&sb, str)
		}
	case RESP_Null_Bulk_String:
		strings.write_rune(&sb, '$')
		strings.write_int(&sb, -1)
		strings.write_string(&sb, "\r\n")
	case RESP_Null_Array:
		strings.write_rune(&sb, '*')
		strings.write_int(&sb, -1)
		strings.write_string(&sb, "\r\n")
	}

	str = strings.clone(strings.to_string(sb))
	return str, ok
}

parse :: proc(buf: []byte, pos: int) -> (RESP, int, RESP_Error) {
	if pos >= len(buf) {
		return {}, -1, .None
	}
	data_type := RESP_Data_Type(buf[pos])

	slice, next_pos, ok := word(buf, pos + 1)
	if !ok {
		return {}, pos, .Unexpected_End
	}

	token: RESP

	#partial switch data_type {
	case .Simple_String:
		str := strings.clone_from_bytes(buf[slice.start:slice.end])
		token = RESP_Simple_String{str}
	case .Simple_Error:
		str := strings.clone_from_bytes(buf[slice.start:slice.end])
		token = RESP_Simple_Error{str}
	case .Integer:
		s, clone_err := strings.clone_from_bytes(buf[slice.start:slice.end])
		i, parse_ok := strconv.parse_i64(s)
		token = RESP_Integer{i}
	case .Bulk_String:
		s, clone_err := strings.clone_from_bytes(buf[slice.start:slice.end])
		count, parse_ok := strconv.parse_int(s)
		if count == -1 {
			_, next_pos, _ = word(buf, next_pos)
			token = RESP_Null_Bulk_String{}
		} else {
			slice, n, ok := word(buf, next_pos)
			str := strings.clone_from_bytes(buf[slice.start:slice.end])
			token = RESP_Bulk_String{str}
			next_pos = n
		}
	case .Array:
		s, clone_err := strings.clone_from_bytes(buf[slice.start:slice.end])
		count, parse_ok := strconv.parse_int(s)
		if count == -1 {
			_, next_pos, _ = word(buf, next_pos)
			token = RESP_Null_Bulk_String{}
		} else {
			arr_token := RESP_Array{}
			arr_token.elements = make(type_of(arr_token.elements))

			for i := 0; i < count; i += 1 {
				t, n, err := parse(buf, next_pos)
				if err != .None {
					return {}, -1, err
				}
				append(&arr_token.elements, t)
				next_pos = n
			}

			token = arr_token
		}
	}

	return token, next_pos, .None
}

word :: proc(buf: []byte, pos: int) -> (Buf_Slice, int, bool) {
	crlf, found := next_crlf(buf, pos)
	if !found {
		return {}, 0, false
	}

	if int(u32(len(buf))) <= pos {
		return {}, 0, false
	}

	next_pos := crlf + 2
	slice := Buf_Slice{pos, crlf}

	return slice, next_pos, true
}

parse_count :: proc(buf: []byte, pos: int) -> (int, int, RESP_Error) {
	w, next, ok := word(buf, pos)
	if !ok {
		return {}, pos, .Unexpected_End
	}

	s, clone_err := strings.clone_from_bytes(buf[w.start:w.end])
	count, parse_ok := strconv.parse_int(s)

	return count, next, .None
}

next_crlf :: proc(buf: []byte, from_pos: int) -> (int, bool) {
	for i := from_pos; i < len(buf); i += 1 {
		if buf[i] == '\r' {
			return i, true
		}
	}

	return -1, false
}
