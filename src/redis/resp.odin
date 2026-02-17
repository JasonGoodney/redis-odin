package redis

import "core:bytes"
import "core:slice"
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

Token :: union {
	String_Token,
	Error_Token,
	Int_Token,
	Array_Token,
	Null_Bulk_String_Token,
	Null_Array_Token,
}

String_Token :: struct {
	value: string,
}

Int_Token :: struct {
	value: i64,
}

Array_Token :: struct {
	value: [dynamic]Token,
}

Error_Token :: distinct String_Token
Null_Bulk_String_Token :: distinct String_Token
Null_Array_Token :: distinct Array_Token

parse :: proc(buf: []byte, pos: int) -> (Token, int, RESP_Error) {
	if pos >= len(buf) {
		return {}, -1, .None
	}
	data_type := RESP_Data_Type(buf[pos])

	slice, next_pos, ok := word(buf, pos + 1)
	if !ok {
		return {}, pos, .Unexpected_End
	}

	token: Token

	#partial switch data_type {
	case .Simple_String:
		str := strings.clone_from_bytes(buf[slice.start:slice.end])
		token = String_Token{str}
	case .Simple_Error:
		str := strings.clone_from_bytes(buf[slice.start:slice.end])
		token = Error_Token{str}
	case .Integer:
		s, clone_err := strings.clone_from_bytes(buf[slice.start:slice.end])
		i, parse_ok := strconv.parse_i64(s)
		token = Int_Token{i}
	case .Bulk_String:
		s, clone_err := strings.clone_from_bytes(buf[slice.start:slice.end])
		count, parse_ok := strconv.parse_int(s)
		if count == -1 {
			_, next_pos, _ = word(buf, next_pos)
			token = Null_Bulk_String_Token{}
		} else {
			slice, n, ok := word(buf, next_pos)
			str := strings.clone_from_bytes(buf[slice.start:slice.end])
			token = String_Token{str}
			next_pos = n
		}
	case .Array:
		s, clone_err := strings.clone_from_bytes(buf[slice.start:slice.end])
		count, parse_ok := strconv.parse_int(s)
		if count == -1 {
			_, next_pos, _ = word(buf, next_pos)
			token = Null_Bulk_String_Token{}
		} else {
			arr_token := Array_Token{}
			arr_token.value = make(type_of(arr_token.value))

			for i := 0; i < count; i += 1 {
				t, n, err := parse(buf, next_pos)
				if err != .None {
					return {}, -1, err
				}
				append(&arr_token.value, t)
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

// parse_simple_string :: proc(buf: []byte, pos: int) -> (RESP_Buf_Slice_String, int, RESP_Error) {
// 	w, next, ok := word(buf, pos)
// 	if !ok {
// 		return {}, pos, .Unexpected_End
// 	}

// 	return {w}, next, .None
// }

// parse_simple_error :: proc(buf: []byte, pos: int) -> (RESP_Buf_Slice_Error, int, RESP_Error) {
// 	w, next, ok := word(buf, pos)
// 	if !ok {
// 		return {}, pos, .Unexpected_End
// 	}

// 	return {w}, next, .None
// }

parse_count :: proc(buf: []byte, pos: int) -> (int, int, RESP_Error) {
	w, next, ok := word(buf, pos)
	if !ok {
		return {}, pos, .Unexpected_End
	}

	s, clone_err := strings.clone_from_bytes(buf[w.start:w.end])
	count, parse_ok := strconv.parse_int(s)

	return count, next, .None
}

// parse_integer :: proc(buf: []byte, pos: int) -> (i64, int, RESP_Error) {
// 	i, next, ok := parse_count(buf, pos)
// 	return i64(i), next, .None
// }

// parse_bulk_string :: proc(
// 	buf: []byte,
// 	pos: int,
// ) -> (
// 	RESP_Buf_Slice_String,
// 	bool,
// 	int,
// 	RESP_Error,
// ) {
// 	count, next, err := parse_count(buf, pos)
// 	if count == -1 {
// 		n, _ := next_crlf(buf, next)
// 		return {}, true, n, .None
// 	}

// 	w, n, ok := word(buf, next)
// 	if !ok {
// 		return {}, false, pos, .Unexpected_End
// 	}


// 	return {w}, false, n, .None
// }

// parse_array :: proc(buf: []byte, pos: int) -> (RESP_Buf_Slice_Array, int, RESP_Error) {
// 	count, next, err := parse_count(buf, pos)
// 	if count == -1 {
// 		n, _ := next_crlf(buf, next)
// 		return {}, n, .None
// 	}

// 	result := RESP_Buf_Slice_Array{}
// 	result.value = make(type_of(result.value), count)

// 	slice: Buf_Slice
// 	ok: bool
// 	for i := 0; i < count; i += 1 {
// 		prev := next
// 		slice, next, ok := word(buf, next)
// 		if !ok {
// 			return {}, prev, .Bad_Array_Size
// 		}

// 		append(&result.value, slice)
// 	}


// 	return result, next, .None
// }

next_crlf :: proc(buf: []byte, from_pos: int) -> (int, bool) {
	for i := from_pos; i < len(buf); i += 1 {
		if buf[i] == '\r' {
			return i, true
		}
	}

	return -1, false
}
