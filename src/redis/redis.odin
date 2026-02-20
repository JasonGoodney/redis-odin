#+feature dynamic-literals

package redis

import "core:fmt"
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
		res_resp, cmd_ok := cmd.handler(database, resp)

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

Command :: struct {
	name:     string,
	min_args: int,
	handler:  proc(db: Database, resp: RESP_Array) -> (RESP, bool),
}

PING :: Command{"PING", 0, ping}
ECHO :: Command{"ECHO", 1, echo}
SET :: Command{"SET", 2, set}
GET :: Command{"GET", 1, get}
RPUSH :: Command{"RPUSH", 2, rpush}

commands := map[string]Command {
	PING.name  = PING,
	ECHO.name  = ECHO,
	SET.name   = SET,
	GET.name   = GET,
	RPUSH.name = RPUSH,
}

ping :: proc(db: Database, resp: RESP_Array) -> (RESP, bool) {
	// assert(len(resp.elements) - 1 == PING.min_args)
	if len(resp.elements) - 1 == 1 {
		return echo(db, resp)
	}
	return RESP_Simple_String{"PONG"}, true
}

echo :: proc(db: Database, resp: RESP_Array) -> (RESP, bool) {
	assert(len(resp.elements) - 1 == ECHO.min_args)
	return resp.elements[1], true
}

set :: proc(db: Database, resp: RESP_Array) -> (RESP, bool) {
	elem_count := len(resp.elements)
	assert(elem_count - 1 >= SET.min_args)

	key := (resp.elements[1].(RESP_Bulk_String)).value
	val := (resp.elements[2].(RESP_Bulk_String)).value

	obj := String_Cachable {
		value = val,
	}

	for i := 3; i < elem_count; i += 1 {
		opt := resp.elements[i].(RESP_Bulk_String)
		opt_key := strings.to_upper(opt.value)
		switch opt_key {
		case "PX":
			i += 1
			if i > elem_count {
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

get :: proc(db: Database, resp: RESP_Array) -> (RESP, bool) {
	assert(len(resp.elements) - 1 == GET.min_args)

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

rpush :: proc(db: Database, resp: RESP_Array) -> (RESP, bool) {
	argc := len(resp.elements)
	assert(argc - 1 >= RPUSH.min_args)

	key := (resp.elements[1].(RESP_Bulk_String)).value
	value_count := argc - 2

	values := make([dynamic]string, value_count)
	defer delete(values)

	if existing_obj, peek_ok := database_peek(&database, key); peek_ok {
		elements := (existing_obj.(List_Cachable)).elements
		append_elems(&values, ..elements)
	}

	for i := 3; i < argc; i += 1 {
		append(&values, (resp.elements[i].(RESP_Bulk_String)).value)
	}

	obj := List_Cachable{values[:]}
	set_ok := database_set(&database, key, obj)
	if !set_ok {
		return {}, false
	}

	return RESP_Integer{i64(value_count)}, true
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
