#+feature dynamic-literals

package redis

import "base:intrinsics"
import "core:container/intrusive/list"
import "core:container/queue"
import "core:fmt"
import "core:math"
import "core:net"
import "core:slice"
import "core:strconv"
import "core:strings"
import "core:thread"
import "core:time"

g_server: Server
Server :: struct {
	socket:      net.TCP_Socket,
	database:    ^Database,
	connections: queue.Queue(^Connection),
}

Connection :: struct {
	server: ^Server,
	socket: net.TCP_Socket,
	buffer: [256]byte,
}

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

	socket, listen_err := net.listen_tcp(endpoint)
	fmt.assertf(listen_err == nil, "Failed to listen on [%s:%d]: %v", ip, port, listen_err)
	g_server.socket = socket
	g_server.database = database_init()

	fmt.printfln("Listening on TCP: %s", net.endpoint_to_string(endpoint))

	retry := 1
	max_retries := 5
	for {
		client, _, err_accept := net.accept_tcp(g_server.socket)
		conn := new(Connection)
		conn.server = &g_server
		conn.socket = client

		if err_accept != nil {
			fmt.println("Failed to accept TCP connection")
			retry += 1
			if retry > max_retries {
				break
			}
			continue
		}

		thread.create_and_start_with_poly_data(conn, handle_msg)
	}

	fmt.println("Closing server.")

	net.close(g_server.socket)
}

handle_msg :: proc(conn: ^Connection) {
	for {
		bytes_recv, err_recv := net.recv_tcp(conn.socket, conn.buffer[:])
		if err_recv != nil {
			fmt.println("Failed to receive data", err_recv)
			break
		}
		received := conn.buffer[:bytes_recv]
		if len(received) == 0 ||
		   is_ctrl_d(received) ||
		   is_empty(received) ||
		   is_telnet_ctrl_c(received) {
			fmt.println("Disconnecting client")
			break
		}

		fmt.printfln("Server received [ %d bytes ]: %s", len(received), received)

		decoded, err := decode(received)
		resp_arr := decoded.(RESP_Array)
		args := slice.mapper(resp_arr.elements[:], proc(bulkstr: RESP) -> string {
			return bulkstr.(RESP_Bulk_String).value
		})

		cmd := commands_table[strings.to_upper(args[0])]
		res_resp, cmd_ok := cmd.handler(conn, args)

		response: string
		if !cmd_ok {
			response = encode(RESP_Simple_Error{"ERROR"})
		} else {
			response = encode(res_resp)
		}

		assert(response != "")

		buffer := transmute([]u8)response
		bytes_sent, err_send := net.send_tcp(conn.socket, buffer)
		if err_send != nil {
			fmt.println("Failed to send data")
		}

		sent := buffer[:bytes_sent]
		fmt.printfln("Server sent [ %d bytes ]: %s", len(sent), sent)
	}

	net.close(conn.socket)
}

Command_Handler :: proc(conn: ^Connection, args: []string) -> (RESP, bool)
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
LPUSH :: Command{"LPUSH", 3, lpush}
LRANGE :: Command{"LRANGE", 4, lrange}
LLEN :: Command{"LLEN", 2, llen}
LPOP :: Command{"LPOP", 2, lpop}
RPOP :: Command{"RPOP", 2, rpop}
BLPOP :: Command{"BLPOP", 3, blpop}
BRPOP :: Command{"BRPOP", 3, brpop}

commands_table := map[string]Command {
	PING.name   = PING,
	ECHO.name   = ECHO,
	SET.name    = SET,
	GET.name    = GET,
	RPUSH.name  = RPUSH,
	LPUSH.name  = LPUSH,
	LRANGE.name = LRANGE,
	LLEN.name   = LLEN,
	LPOP.name   = LPOP,
	RPOP.name   = RPOP,
	BLPOP.name  = BLPOP,
	BRPOP.name  = BRPOP,
}


ping :: proc(conn: ^Connection, args: []string) -> (RESP, bool) {
	if len(args) > 1 {
		return echo(conn, args)
	}
	return RESP_Simple_String{"PONG"}, true
}

echo :: proc(conn: ^Connection, args: []string) -> (RESP, bool) {
	assert(len(args) == ECHO.min_args)
	return RESP_Bulk_String{args[1]}, true
}

set :: proc(conn: ^Connection, args: []string) -> (RESP, bool) {
	argc := len(args)
	assert(argc >= SET.min_args)

	key := args[1]
	val := args[2]

	obj := String_Value {
		value = val,
	}

	for i := 3; i < argc; i += 1 {
		opt := strings.to_upper(args[i])
		switch opt {
		case "PX":
			i += 1
			if i > argc {
				continue
			}
			opt_val := args[i]
			ms, ok := strconv.parse_i64(opt_val)
			if ok && ms > 0 {
				expires_at := time.time_add(time.now(), time.Duration(ms * 1e6))
				obj.expires_at = expires_at
			}
		case:
			break
		}
	}

	set_ok := database_set(conn.server.database, key, obj)
	if !set_ok {
		return {}, false
	}

	return RESP_Simple_String{"OK"}, true
}

get :: proc(conn: ^Connection, args: []string) -> (RESP, bool) {
	assert(len(args) == GET.min_args)

	key := args[1]
	val, get_ok := database_get(conn.server.database, key)
	if !get_ok {
		fmt.printfln("Item not found for %s", key)
		return {}, false
	}

	str := val.(String_Value)
	if str.expires_at != {} && time.diff(time.now(), str.expires_at) < 0 {
		fmt.printfln("Item expired for %s", key)
		database_remove(conn.server.database, key)
		return RESP_Null_Bulk_String{}, true
	}

	return RESP_Bulk_String{str.value}, true
}

rpush :: proc(conn: ^Connection, args: []string) -> (RESP, bool) {
	return push(conn, args, list_append)
}

lpush :: proc(conn: ^Connection, args: []string) -> (RESP, bool) {
	return push(conn, args, list_prepend)
}

push :: proc(
	conn: ^Connection,
	args: []string,
	pusher: proc(list: ^List_Value, value: string),
) -> (
	RESP,
	bool,
) {
	argc := len(args)
	assert(argc >= RPUSH.min_args)

	key := args[1]
	values_count := argc - 2

	list_obj: List_Value

	if existing_obj, peek_ok := database_peek(conn.server.database, key); peek_ok {
		list_obj = existing_obj.(List_Value)
	} else {
		list_obj = list_init()
	}

	for i := 2; i < argc; i += 1 {
		value := args[i]
		pusher(&list_obj, value)
	}

	set_ok := database_set(conn.server.database, key, list_obj)
	if !set_ok {
		return {}, false
	}

	return RESP_Integer{i64(list_obj.len)}, true
}

lrange :: proc(conn: ^Connection, args: []string) -> (RESP, bool) {
	argc := len(args)
	assert(argc == LRANGE.min_args)

	key := args[1]
	start_str := args[2]
	stop_str := args[3]
	start, start_ok := strconv.parse_int(start_str)
	stop, stop_ok := strconv.parse_int(stop_str)

	obj, get_ok := database_get(conn.server.database, key)
	if !get_ok {
		return RESP_Array{}, true
	}

	list_obj := obj.(List_Value)
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

	iter := list.iterator_head(list_obj.elements^, List_Element, "node")
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

llen :: proc(conn: ^Connection, args: []string) -> (RESP, bool) {
	argc := len(args)
	assert(argc == LLEN.min_args)

	key := args[1]

	obj, get_ok := database_get(conn.server.database, key)
	if !get_ok {
		return RESP_Integer{0}, true
	}

	list, cast_ok := obj.(List_Value)
	if !cast_ok {
		return RESP_Simple_Error{"Not a list"}, true
	}

	return RESP_Integer{i64(list.len)}, true
}

lpop :: proc(conn: ^Connection, args: []string) -> (RESP, bool) {
	return pop(LPOP, conn, args, list_pop_front)
}

rpop :: proc(conn: ^Connection, args: []string) -> (RESP, bool) {
	return pop(RPOP, conn, args, list_pop_back)
}

pop :: proc(
	cmd: Command,
	conn: ^Connection,
	args: []string,
	popper: proc(l: ^List_Value, count: int = 1) -> []string,
) -> (
	RESP,
	bool,
) {
	argc := len(args)
	if (argc < cmd.min_args) {
		fmt.printfln("Usage: %s key [count]", cmd.name)
		return {}, false
	}

	key := args[1]
	count := 1
	if (argc > cmd.min_args) {
		c, ok := strconv.parse_int(args[2])
		if ok {
			count = c
		}
	}

	obj, get_ok := database_get(conn.server.database, key)
	if !get_ok {
		return RESP_Null_Bulk_String{}, true
	}

	list, cast_ok := obj.(List_Value)
	if !cast_ok {
		return RESP_Null_Bulk_String{}, true
	}

	popped := popper(&list, count)
	if len(popped) == 1 {
		return RESP_Bulk_String{popped[0]}, true
	} else {
		bulkstrs := make([dynamic]RESP)
		for str in popped {
			append(&bulkstrs, RESP_Bulk_String{str})
		}
		return RESP_Array{bulkstrs}, true
	}
}

blpop :: proc(conn: ^Connection, args: []string) -> (RESP, bool) {
	return bpop(BLPOP, conn, args, list_pop_front)
}

brpop :: proc(conn: ^Connection, args: []string) -> (RESP, bool) {
	return bpop(BRPOP, conn, args, list_pop_back)
}

bpop :: proc(
	cmd: Command,
	conn: ^Connection,
	args: []string,
	callback: proc(l: ^List_Value, count: int = 1) -> []string,
) -> (
	RESP,
	bool,
) {
	argc := len(args)
	if (argc < cmd.min_args) {
		fmt.printfln("Usage: %s key [key ...] timeout", cmd.name)
		return {}, false
	}

	key := args[1]
	timeout, parse_ok := strconv.parse_int(args[2])

	return {}, false
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
