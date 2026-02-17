package tests_redis

import "core:bytes"
import "core:strings"
import "core:testing"

import "../../src/redis"

@(test)
test_next_crlf :: proc(t: ^testing.T) {
	buf: []byte = {'+', 'h', 'e', 'l', 'l', 'o', '\r', '\n'}
	pos: int = 1
	next, ok := redis.next_crlf(buf, pos)

	testing.expect(t, next == 6)
}

@(test)
test_parse_word :: proc(t: ^testing.T) {
	buf: []byte = {'+', 'h', 'e', 'l', 'l', 'o', '\r', '\n'}
	pos: int = 1
	slice, next, ok := redis.word(buf, pos)

	testing.expect(t, next == 8)
	testing.expect(t, slice.start == 1)
	testing.expect(t, slice.end == 6)
}

@(test)
test_parse_count :: proc(t: ^testing.T) {
	buf := bytes.Buffer{}
	bytes.buffer_write_string(&buf, "$3\r\nFOO\r\n")
	pos: int = 1
	c, next, err := redis.parse_count(buf.buf[:], pos)

	testing.expect(t, c == 3)
	testing.expect(t, next == 4)
}

@(test)
test_parse_token_simple_string :: proc(t: ^testing.T) {
	buf: []byte = {'+', 'h', 'e', 'l', 'l', 'o', '\r', '\n'}
	pos: int = 0
	token, next, err := redis.parse(buf, pos)

	#partial switch tok in token {
	case redis.String_Token:
		testing.expect(t, next == 8)
		testing.expect(t, 0 == strings.compare(tok.value, "hello"))
	}
}

@(test)
test_parse_token_simple_error :: proc(t: ^testing.T) {
	buf: []byte = {'-', 'e', 'r', 'r', 'o', 'r', '\r', '\n'}
	pos: int = 0
	token, next, err := redis.parse(buf, pos)

	#partial switch tok in token {
	case redis.Error_Token:
		testing.expect(t, next == 8)
		testing.expect(t, 0 == strings.compare(tok.value, "error"))
	}
}

@(test)
test_parse_token_integer :: proc(t: ^testing.T) {
	{
		buf := string_to_bytes(":1000\r\n")
		pos: int = 0
		token, next, err := redis.parse(buf, pos)

		#partial switch tok in token {
		case redis.Int_Token:
			testing.expect(t, next == len(buf))
			testing.expect(t, tok.value == 1000)
		}
	}

	{
		buf := string_to_bytes(":3\r\n")
		pos: int = 0
		token, next, err := redis.parse(buf, pos)

		#partial switch tok in token {
		case redis.Int_Token:
			testing.expect(t, next == len(buf))
			testing.expect(t, tok.value == 3)
		}
	}

	{
		buf := string_to_bytes(":-10\r\n")
		pos: int = 0
		token, next, err := redis.parse(buf, pos)

		#partial switch tok in token {
		case redis.Int_Token:
			testing.expect(t, next == len(buf))
			testing.expect(t, tok.value == -10)
		}
	}
}

@(test)
test_parse_token_bulk_string :: proc(t: ^testing.T) {
	{
		buf := string_to_bytes("$3\r\nfoo\r\n")
		pos: int = 0
		token, next, err := redis.parse(buf, pos)

		#partial switch tok in token {
		case redis.String_Token:
			testing.expect(t, next == len(buf))
			testing.expect(t, 0 == strings.compare(tok.value, "foo"))
		}
	}

	{
		buf := string_to_bytes("$-1\r\n\r\n")
		pos: int = 0
		token, next, err := redis.parse(buf, pos)

		#partial switch tok in token {
		case redis.Null_Bulk_String_Token:
			testing.expect(t, next == len(buf))
			testing.expect(t, len(tok.value) == 0)
		}
	}
}

@(test)
test_parse_token_array :: proc(t: ^testing.T) {
	{
		buf := string_to_bytes("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
		pos: int = 0
		token, next, err := redis.parse(buf, pos)

		testing.expect(t, err == .None)

		#partial switch tok in token {
		case redis.Array_Token:
			testing.expect(t, len(tok.value) == 2)
		}
	}

	{
		buf := string_to_bytes("*0\r\n")
		pos: int = 0
		token, next, err := redis.parse(buf, pos)

		testing.expect(t, err == .None)

		#partial switch tok in token {
		case redis.Array_Token:
			testing.expect(t, len(tok.value) == 0)
		}
	}

	{
		buf := string_to_bytes("*3\r\n:1\r\n:2\r\n:3\r\n")
		pos: int = 0
		token, next, err := redis.parse(buf, pos)

		testing.expect(t, err == .None)

		#partial switch tok in token {
		case redis.Array_Token:
			testing.expect(t, len(tok.value) == 3)

			#partial switch v in tok.value[0] {
			case redis.Int_Token:
				testing.expect(t, v.value == 1)
			}

			#partial switch v in tok.value[1] {
			case redis.Int_Token:
				testing.expect(t, v.value == 2)
			}

			#partial switch v in tok.value[2] {
			case redis.Int_Token:
				testing.expect(t, v.value == 3)
			}
		}
	}

	{
		buf := string_to_bytes("*-1\r\n")
		pos: int = 0
		token, next, err := redis.parse(buf, pos)

		testing.expect(t, err == .None)

		#partial switch tok in token {
		case redis.Null_Array_Token:
			testing.expect(t, len(tok.value) == 0)
		}
	}
}

string_to_bytes :: proc(s: string) -> []byte {
	b := bytes.Buffer{}
	bytes.buffer_init_string(&b, s)
	defer bytes.buffer_destroy(&b)
	res := bytes.clone(b.buf[:])
	return res
}
