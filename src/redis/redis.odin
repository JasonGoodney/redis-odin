#+feature dynamic-literals

package redis

import "core:container/intrusive/list"
import "core:fmt"
import "core:math"
import "core:net"
import "core:strconv"
import "core:strings"
import "core:thread"
import "core:time"

Client :: struct {
	socket:   net.TCP_Socket,
	database: ^Database,
}

database: Database

connect :: proc(ip: string, port: int) {
	local_addr, addr_ok := net.parse_ip4_address(ip)
	if !addr_ok {
		fmt.println("Failed to parse IP address")
		return
	}

	endpoint := net.Endpoint {
		address = local_addr,
		port    = port,
	}

	sock, err := net.listen_tcp(endpoint)
	if err != nil {
		fmt.println("Failed to listen on TCP", err)
		return
	}

	database = database_init(allocator = context.allocator)

	fmt.printfln("Listening on TCP: %s", net.endpoint_to_string(endpoint))

	retry := 1
	max_retries := 5
	for {
		cli, _, err_accept := net.accept_tcp(sock)

		if err_accept != nil {
			fmt.println("Failed to accept TCP connection")
			retry += 1
			if retry > max_retries {
				break
			}
			continue
		}

		thread.create_and_start_with_poly_data(cli, handle_msg)
	}

	net.close(sock)
}

handle_msg :: proc(client: net.TCP_Socket) {
	buffer: [256]u8
	for {
		bytes_recv, err_recv := net.recv_tcp(client, buffer[:])
		if err_recv != nil {
			fmt.println("Failed to receive data", err_recv)
			break
		}
		received := buffer[:bytes_recv]
		if len(received) == 0 ||
		   is_ctrl_d(received) ||
		   is_empty(received) ||
		   is_telnet_ctrl_c(received) {
			fmt.println("Disconnecting client")
			break
		}

		fmt.printfln("Server received [ %d bytes ]: %s", len(received), received)

		decoded, err := decode(received)
		resp := decoded.(RESP_Array)

		name := (resp.elements[0].(RESP_Bulk_String)).value
		cmd := commands[strings.to_upper(name)]
		res_resp, cmd_ok := cmd.handler(&database, resp)

		response: string
		if !cmd_ok {
			response = encode(RESP_Simple_Error{"ERROR"})
		} else {
			response = encode(res_resp)
		}

		assert(response != "")

		buffer := transmute([]u8)response
		bytes_sent, err_send := net.send_tcp(client, buffer)
		if err_send != nil {
			fmt.println("Failed to send data")
		}

		sent := buffer[:bytes_sent]
		fmt.printfln("Server sent [ %d bytes ]: %s", len(sent), sent)
	}

	net.close(client)
}

Command_Handler :: proc(db: ^Database, resp: RESP_Array) -> (RESP, bool)
Command :: struct {
	name:     string,
	min_args: int,
	handler:  Command_Handler,
}

PING :: Command{"PING", 1, ping}
ECHO :: Command{"ECHO", 2, echo}
SET :: Command{"SET", 3, set}
GET :: Command{"GET", 2, get}
RPUSH :: Command{"RPUSH", 3, rpush}
LRANGE :: Command{"LRANGE", 4, lrange}
LPUSH :: Command{"LPUSH", 3, lpush}

commands := map[string]Command {
	PING.name   = PING,
	ECHO.name   = ECHO,
	SET.name    = SET,
	GET.name    = GET,
	RPUSH.name  = RPUSH,
	LRANGE.name = LRANGE,
	LPUSH.name  = LPUSH,
}

ping :: proc(db: ^Database, resp: RESP_Array) -> (RESP, bool) {
	if len(resp.elements) > 1 {
		return echo(db, resp)
	}
	return RESP_Simple_String{"PONG"}, true
}

echo :: proc(db: ^Database, resp: RESP_Array) -> (RESP, bool) {
	assert(len(resp.elements) == ECHO.min_args)
	return resp.elements[1], true
}

set :: proc(db: ^Database, resp: RESP_Array) -> (RESP, bool) {
	argc := len(resp.elements)
	assert(argc >= SET.min_args)

	key := (resp.elements[1].(RESP_Bulk_String)).value
	val := (resp.elements[2].(RESP_Bulk_String)).value

	obj := String_Cachable {
		value = val,
	}

	for i := 3; i < argc; i += 1 {
		opt := resp.elements[i].(RESP_Bulk_String)
		opt_key := strings.to_upper(opt.value)
		switch opt_key {
		case "PX":
			i += 1
			if i > argc {
				continue
			}
			opt_val := resp.elements[i].(RESP_Bulk_String)
			ms, ok := strconv.parse_i64(opt_val.value)
			if ok && ms > 0 {
				expires_at := time.time_add(time.now(), time.Duration(ms * 1e6))
				obj.expires_at = expires_at
			}
		case:
			break
		}
	}

	set_ok := database_set(&database, key, obj)
	if !set_ok {
		return {}, false
	}

	return RESP_Simple_String{"OK"}, true
}

get :: proc(db: ^Database, resp: RESP_Array) -> (RESP, bool) {
	assert(len(resp.elements) == GET.min_args)

	key := (resp.elements[1].(RESP_Bulk_String)).value
	val, get_ok := database_get(&database, key)
	if !get_ok {
		fmt.printfln("Item not found for %s", key)
		return {}, false
	}

	str := val.(String_Cachable)
	if str.expires_at != {} && time.diff(time.now(), str.expires_at) < 0 {
		fmt.printfln("Item expired for %s", key)
		database_remove(&database, key)
		return RESP_Null_Bulk_String{}, true
	}

	return RESP_Bulk_String{str.value}, true
}

rpush :: proc(db: ^Database, resp: RESP_Array) -> (RESP, bool) {
	return push(db, resp, list_append)
}

lpush :: proc(db: ^Database, resp: RESP_Array) -> (RESP, bool) {
	return push(db, resp, list_prepend)
}

push :: proc(
	db: ^Database,
	resp: RESP_Array,
	push_proc: proc(list: ^List, value: string),
) -> (
	RESP,
	bool,
) {
	argc := len(resp.elements)
	assert(argc >= RPUSH.min_args)

	key := (resp.elements[1].(RESP_Bulk_String)).value
	values_count := argc - 2

	list_obj: List

	if existing_obj, peek_ok := database_peek(&database, key); peek_ok {
		list_obj = existing_obj.(List)
	} else {
		list_obj = list_init()
	}

	for i := 2; i < argc; i += 1 {
		value := (resp.elements[i].(RESP_Bulk_String)).value
		push_proc(&list_obj, value)
	}

	set_ok := database_set(&database, key, list_obj)
	if !set_ok {
		return {}, false
	}

	return RESP_Integer{i64(list_obj.len)}, true
}

lrange :: proc(db: ^Database, resp: RESP_Array) -> (RESP, bool) {
	argc := len(resp.elements)
	assert(argc == LRANGE.min_args)

	key := (resp.elements[1].(RESP_Bulk_String)).value
	start_str := (resp.elements[2].(RESP_Bulk_String)).value
	stop_str := (resp.elements[3].(RESP_Bulk_String)).value
	start, start_ok := strconv.parse_int(start_str)
	stop, stop_ok := strconv.parse_int(stop_str)

	obj, get_ok := database_get(db, key)
	if !get_ok {
		return RESP_Array{}, true
	}

	list_obj := obj.(List)
	elem_count := list_obj.len

	if start > elem_count {
		return RESP_Array{}, true
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

	values := make([dynamic]RESP)
	//	defer delete(values)

	iter := list.iterator_head(list_obj.elements^, List_Item, "node")
	i := 0
	for elem in list.iterate_next(&iter) {
		if i < start {
			i += 1
			continue
		}
		if i > stop {
			break
		}
		append(&values, RESP_Bulk_String{elem.value})
		i += 1
	}

	return RESP_Array{values}, true
}

is_ctrl_d :: proc(bytes: []u8) -> bool {
	return len(bytes) == 1 && bytes[0] == 4
}

is_empty :: proc(bytes: []u8) -> bool {
	return(
		(len(bytes) == 2 && bytes[0] == '\r' && bytes[1] == '\n') ||
		(len(bytes) == 1 && bytes[0] == '\n') \
	)
}

is_telnet_ctrl_c :: proc(bytes: []u8) -> bool {
	return(
		(len(bytes) == 3 && bytes[0] == 255 && bytes[1] == 251 && bytes[2] == 6) ||
		(len(bytes) == 5 &&
				bytes[0] == 255 &&
				bytes[1] == 244 &&
				bytes[2] == 255 &&
				bytes[3] == 253 &&
				bytes[4] == 6) \
	)
}

insensitive_compare :: proc(lhs: string, rhs: string) -> bool {
	return 0 == strings.compare(strings.to_lower(lhs), strings.to_lower(rhs))
}
