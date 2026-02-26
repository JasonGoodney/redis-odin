#+feature dynamic-literals

package redis

import "core:container/queue"
import "core:fmt"
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

		response: string
		err_msg, ok := check_command_usage(args)
		if !ok {
			// Outputs to the server.
			// Fix to output to the client
			fmt.println(err_msg)
			break
		}

		cmd := commands_table[strings.to_upper(args[0])]
		resp := cmd.handler(conn, args)
		response = encode(resp)

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

Command_Handler :: proc(conn: ^Connection, args: []string) -> RESP
Command :: struct {
	name:      string,
	min_args:  int,
	handler:   Command_Handler,
	arguments: []string,
}

commands_table := map[string]Command {
	"PING"   = Command{"PING", 1, ping, {"[message]"}},
	"ECHO"   = Command{"ECHO", 2, echo, {"message"}},
	"SET"    = Command{"SET", 3, set, {"key", "value", "[options]"}},
	"GET"    = Command{"GET", 2, get, {"get"}},
	"RPUSH"  = Command{"RPUSH", 3, rpush, {"key", "element", "[element ...]"}},
	"LPUSH"  = Command{"LPUSH", 3, lpush, {"key", "element", "[element ...]"}},
	"LRANGE" = Command{"LRANGE", 4, lrange, {"key", "start", "stop"}},
	"LLEN"   = Command{"LLEN", 2, llen, {"key"}},
	"LPOP"   = Command{"LPOP", 2, lpop, {"key", "[count]"}},
	"RPOP"   = Command{"RPOP", 2, rpop, {"key", "count"}},
	"BLPOP"  = Command{"BLPOP", 3, blpop, {"key", "[key ...]", "timeout"}},
	"BRPOP"  = Command{"BRPOP", 3, brpop, {"key", "[key ...]", "timeout"}},
	"TYPE"   = Command{"TYPE", 2, type, {"key"}},
	"XADD"   = Command {
		"XADD",
		5,
		xadd,
		{"key", "<* | id>", "field", "value", "[field value ...]"},
	},
	"XRANGE" = Command{"XRANGE", 4, xrange, {"key", "start", "end", "[COUNT count]"}},
}

check_command_usage :: proc(args: []string) -> (message: string, ok: bool) {
	argc := len(args)
	if argc == 0 {
		return "Usage: COMMAND args...", false
	}
	cmd := commands_table[strings.to_upper(args[0])]
	if argc < cmd.min_args {
		str := fmt.tprintf("Usage: %s %s", cmd.name, strings.join(cmd.arguments, " "))
		return str, false
	}

	return "", true
}

ping :: proc(conn: ^Connection, args: []string) -> RESP {
	if len(args) > 1 {
		return echo(conn, args)
	}
	return RESP_Simple_String{"PONG"}
}

echo :: proc(conn: ^Connection, args: []string) -> RESP {
	return RESP_Bulk_String{args[1]}
}

set :: proc(conn: ^Connection, args: []string) -> RESP {
	argc := len(args)
	key := args[1]
	val := args[2]

	obj := String {
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

	err := string_set(conn.server.database, key, obj)
	if err != nil {
		return {}
	}

	return RESP_Simple_String{"OK"}
}

get :: proc(conn: ^Connection, args: []string) -> RESP {
	key := args[1]
	val, err := string_get(conn.server.database, key)
	switch err {
	case .Expired:
		return RESP_Null_Bulk_String{}
	case .Key_Not_Found, .Mismatch_Type:
		return RESP_Simple_Error{"(error) Key not found"}
	case .None:
		return RESP_Bulk_String{val.value}
	}

	return RESP_Null_Bulk_String{}
}

rpush :: proc(conn: ^Connection, args: []string) -> RESP {
	count, _ := list_push_back(conn.server.database, args[1], ..args[2:])
	return RESP_Integer{count}
}

lpush :: proc(conn: ^Connection, args: []string) -> RESP {
	count, _ := list_push_front(conn.server.database, args[1], ..args[2:])
	return RESP_Integer{count}
}

lrange :: proc(conn: ^Connection, args: []string) -> RESP {
	key := args[1]
	start, start_ok := strconv.parse_i64(args[2])
	stop, stop_ok := strconv.parse_i64(args[3])

	values, _ := list_range(conn.server.database, key, start, stop)
	resp := RESP_Array{}
	resp.elements = make(type_of(resp.elements))
	for val in values {
		append(&resp.elements, RESP_Bulk_String{val})
	}

	return resp
}

llen :: proc(conn: ^Connection, args: []string) -> RESP {
	length, _ := list_length(conn.server.database, args[1])
	return RESP_Integer{length}
}

lpop :: proc(conn: ^Connection, args: []string) -> RESP {
	return gen_pop(conn, args, list_pop_front)
}

rpop :: proc(conn: ^Connection, args: []string) -> RESP {
	return gen_pop(conn, args, list_pop_back)
}

gen_pop :: proc(
	conn: ^Connection,
	args: []string,
	popper: proc(db: ^Database, key: string, count: int = 1) -> ([]string, Database_Error),
) -> RESP {
	key := args[1]
	count := 1
	if len(args) > 2 {
		c, ok := strconv.parse_int(args[2])
		if ok {
			count = c
		}
	}

	popped, err := popper(conn.server.database, key, count)
	if len(popped) == 1 {
		return RESP_Bulk_String{popped[0]}
	} else {
		bulkstrs := make([dynamic]RESP)
		for str in popped {
			append(&bulkstrs, RESP_Bulk_String{str})
		}
		return RESP_Array{bulkstrs}
	}
}

blpop :: proc(conn: ^Connection, args: []string) -> RESP {
	return bpop(conn, args, list_pop_front)
}

brpop :: proc(conn: ^Connection, args: []string) -> RESP {
	return bpop(conn, args, list_pop_back)
}

bpop :: proc(
	conn: ^Connection,
	args: []string,
	popper: proc(db: ^Database, key: string, count: int = 1) -> ([]string, Database_Error),
) -> RESP {
	key := args[1]

	timeout_s, parse_ok := strconv.parse_f64(args[2])
	if !parse_ok {
		return RESP_Simple_Error{"Bad timeout"}
	}

	for i: f64 = 0; true; i += 0.1 {
		if timeout_s > 0 && i > timeout_s {
			break
		}

		list, _ := list_get(conn.server.database, key)
		if list.len > 0 {
			popped, ok := popper(conn.server.database, key, 1)
			resp := RESP_Array{}
			append(&resp.elements, RESP_Bulk_String{key})
			append(&resp.elements, RESP_Bulk_String{popped[0]})
			return resp
		}

		time.sleep(100 * time.Millisecond)
	}

	return RESP_Null_Array{}
}

type :: proc(conn: ^Connection, args: []string) -> RESP {
	type, err := generic_type(conn.server.database, args[1])
	if err != nil {
		return RESP_Simple_String{"none"}
	}

	return RESP_Simple_String{type}
}

xadd :: proc(conn: ^Connection, args: []string) -> RESP {
	field_args := args[3:][:]
	fields := make(map[string]string)
	for i := 1; i < len(field_args); i += 2 {
		key := field_args[i - 1]
		val := field_args[i]
		fields[key] = val
	}

	entry, err := stream_add(conn.server.database, args[1], args[2], fields)
	if err != nil {
		return RESP_Simple_Error{error_string(err)}
	}
	return RESP_Bulk_String{stream_entry_id_string(entry.id)}
}

xrange :: proc(conn: ^Connection, args: []string) -> RESP {
	entries, err := stream_range(conn.server.database, args[1], args[2], args[3])
	if err != nil {
		return RESP_Null_Array{}
	}

	result := RESP_Array{}
	result.elements = make(type_of(result.elements))

	for entry in entries {
		entry_arr := RESP_Array{}
		entry_arr.elements = make(type_of(entry_arr.elements))
		id := stream_entry_id_string(entry.id)
		append(&entry_arr.elements, RESP_Bulk_String{id})

		fields_arr := RESP_Array{}
		fields_arr.elements = make(type_of(fields_arr.elements))
		for k, v in entry.fields {
			append(&fields_arr.elements, RESP_Bulk_String{k})
			append(&fields_arr.elements, RESP_Bulk_String{v})
		}

		append(&entry_arr.elements, fields_arr)
		append(&result.elements, entry_arr)
	}


	return result
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

