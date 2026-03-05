#+feature dynamic-literals

package redis

import "core:fmt"
import "core:net"
import "core:slice"
import "core:strconv"
import "core:strings"
import "core:thread"
import "core:time"

server: Server
Server :: struct {
	socket:   net.TCP_Socket,
	database: ^Database,
	clients:  [1024]^Client,
}

Client_Role :: enum {
	Master,
	Slave,
}

Client_State :: enum {
	Normal = 0,
	Transaction,
}

Client :: struct {
	server:       ^Server,
	socket:       net.TCP_Socket,
	role:         Client_Role,
	buffer:       [256]byte,
	state:        Client_State,
	transactions: [dynamic]Transaction,
}

connect :: proc(ip: string, port: int, role: Client_Role) {
	local_addr, addr_ok := net.parse_ip4_address(ip)
	if !addr_ok {
		fmt.println("Failed to parse IP address")
		return
	}

	endpoint := net.Endpoint {
		address = local_addr,
		port    = port,
	}

	server_socket, listen_err := net.listen_tcp(endpoint)
	fmt.assertf(listen_err == nil, "Failed to listen on [%s:%d]: %v", ip, port, listen_err)
	server.socket = server_socket
	server.database = database_init()

	fmt.printfln("Listening on TCP: %s", net.endpoint_to_string(endpoint))

	retry := 1
	max_retries := 5
	for {
		client_socket, _, err_accept := net.accept_tcp(server.socket)
		if err_accept != nil {
			fmt.println("Failed to accept TCP connection")
			retry += 1
			if retry > max_retries {
				break
			}
			continue
		}
		client := server.clients[client_socket]
		if client == nil {
			client = new(Client)
			client.server = &server
			client.role = role
			client.socket = client_socket
			server.clients[client_socket] = client
		}

		fmt.printfln("Client connected: [fd=%d]", client_socket)
		thread.create_and_start_with_poly_data(client, handle_connection)
	}

	fmt.println("Closing server.")

	net.close(server.socket)
}

handle_connection :: proc(client: ^Client) {
	defer {
		fmt.println("Disconnecting client")
		server.clients[client.socket] = nil
		net.close(client.socket)
	}
	for {
		bytes_recv, err_recv := net.recv_tcp(client.socket, client.buffer[:])
		if err_recv != nil {
			fmt.println("Failed to receive data", err_recv)
			break
		}
		received := client.buffer[:bytes_recv]
		if len(received) == 0 ||
		   is_ctrl_d(received) ||
		   is_empty(received) ||
		   is_telnet_ctrl_c(received) {
			break
		}

		fmt.printfln("Server received [ %d bytes ]: %s", len(received), received)

		decoded, err := decode(received)
		if err != nil {
			fmt.eprintfln("decode error: %v", err)
			break
		}
		resp_arr := decoded.(RESP_Array)
		args := slice.mapper(resp_arr.elements[:], proc(bulkstr: RESP) -> string {
			return bulkstr.(RESP_Bulk_String).value
		})

		resp: RESP

		err_msg, ok := check_command_usage(args)
		if !ok {
			resp = RESP_Simple_Error{err_msg}
		}

		cmd, cmd_ok := get_command(args[0])
		if !cmd_ok {
			fmt.printfln("Unsupported command: %s", args[0])
			break
		}
		if client.state == .Transaction && cmd.name != "EXEC" && cmd.name != "DISCARD" {
			resp = transaction_queue(client, args)
		} else {
			resp = cmd.handler(client, args)
		}
		response := encode(resp)
		if response == "" {
			break
		}

		buffer := transmute([]u8)response
		bytes_sent, err_send := net.send_tcp(client.socket, buffer)
		if err_send != nil {
			fmt.println("Failed to send data")
		}

		sent := buffer[:bytes_sent]
		fmt.printfln("Server sent [ %d bytes ]: %s", len(sent), sent)
	}
}

Transaction :: struct {
	cmd:  Command,
	args: []string,
}

Command_Handler :: proc(client: ^Client, args: []string) -> RESP
Command :: struct {
	name:      string,
	min_args:  int,
	handler:   Command_Handler,
	arguments: []string,
}
get_command :: proc(name: string) -> (cmd: Command, ok: bool) #optional_ok {
	return commands_table[strings.to_upper(name)]
}

commands_table := map[string]Command {
	"PING"    = Command{"PING", 1, ping, {"[message]"}},
	"ECHO"    = Command{"ECHO", 2, echo, {"message"}},
	"SET"     = Command{"SET", 3, set, {"key", "value", "[options]"}},
	"GET"     = Command{"GET", 2, get, {"get"}},
	"RPUSH"   = Command{"RPUSH", 3, rpush, {"key", "element", "[element ...]"}},
	"LPUSH"   = Command{"LPUSH", 3, lpush, {"key", "element", "[element ...]"}},
	"LRANGE"  = Command{"LRANGE", 4, lrange, {"key", "start", "stop"}},
	"LLEN"    = Command{"LLEN", 2, llen, {"key"}},
	"LPOP"    = Command{"LPOP", 2, lpop, {"key", "[count]"}},
	"RPOP"    = Command{"RPOP", 2, rpop, {"key", "count"}},
	"BLPOP"   = Command{"BLPOP", 3, blpop, {"key", "[key ...]", "timeout"}},
	"BRPOP"   = Command{"BRPOP", 3, brpop, {"key", "[key ...]", "timeout"}},
	"TYPE"    = Command{"TYPE", 2, type, {"key"}},
	"XADD"    = Command {
		"XADD",
		5,
		xadd,
		{"key", "<* | id>", "field", "value", "[field value ...]"},
	},
	"XRANGE"  = Command{"XRANGE", 4, xrange, {"key", "start", "end", "[COUNT count]"}},
	"XREAD"   = Command{"XREAD", 4, xread, {"STREAMS", "key", "id"}},
	"INCR"    = Command{"INCR", 2, incr, {"key"}},
	"MULTI"   = Command{"MULTI", 1, multi, {}},
	"EXEC"    = Command{"EXEC", 1, exec, {}},
	"DISCARD" = Command{"DISCARD", 1, discard, {}},
	"INFO"    = Command{"INFO", 1, info, {"[section [section ...]]"}},
}

check_command_usage :: proc(args: []string) -> (message: string, ok: bool) {
	argc := len(args)
	if argc == 0 {
		return "Usage: COMMAND args...", false
	}
	cmd := commands_table[strings.to_upper(args[0])]
	if argc < cmd.min_args {
		str := fmt.tprintf(
			"ERR wrong number of argument for '%s' command",
			cmd.name,
			strings.join(cmd.arguments, " "),
		)
		return str, false
	}

	return "", true
}

ping :: proc(client: ^Client, args: []string) -> RESP {
	if len(args) > 1 {
		return echo(client, args)
	}
	return RESP_Simple_String{"PONG"}
}

echo :: proc(client: ^Client, args: []string) -> RESP {
	return RESP_Bulk_String{args[1]}
}

set :: proc(client: ^Client, args: []string) -> RESP {
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

	err := string_set(client.server.database, key, obj)
	if err != nil {
		return {}
	}

	return RESP_Simple_String{"OK"}
}

get :: proc(client: ^Client, args: []string) -> RESP {
	key := args[1]
	val, err := string_get(client.server.database, key)
	switch err {
	case .Expired:
		return RESP_Null_Bulk_String{}
	case .Key_Not_Found:
		// return RESP_Simple_Error{"Key not found"}
		return RESP_Null_Bulk_String{}
	case .Mismatch_Type:
		return RESP_Simple_Error {
			"WRONGTYPE Operation against a key holding the wrong kind of value",
		}
	case .None:
		return RESP_Bulk_String{val.value}
	}

	return RESP_Null_Bulk_String{}
}

incr :: proc(client: ^Client, args: []string) -> RESP {
	val, incr_err := string_incr(client.server.database, args[1])
	if incr_err != nil {
		return RESP_Simple_Error{"ERR value is not an integer or out of range"}
	}
	return RESP_Integer{val}
}

rpush :: proc(client: ^Client, args: []string) -> RESP {
	count, _ := list_push_back(client.server.database, args[1], ..args[2:])
	return RESP_Integer{count}
}

lpush :: proc(client: ^Client, args: []string) -> RESP {
	count, _ := list_push_front(client.server.database, args[1], ..args[2:])
	return RESP_Integer{count}
}

lrange :: proc(client: ^Client, args: []string) -> RESP {
	key := args[1]
	start, start_ok := strconv.parse_i64(args[2])
	stop, stop_ok := strconv.parse_i64(args[3])

	values, _ := list_range(client.server.database, key, start, stop)
	resp := RESP_Array{}
	resp.elements = make(type_of(resp.elements))
	for val in values {
		append(&resp.elements, RESP_Bulk_String{val})
	}

	return resp
}

llen :: proc(client: ^Client, args: []string) -> RESP {
	length, _ := list_length(client.server.database, args[1])
	return RESP_Integer{length}
}

lpop :: proc(client: ^Client, args: []string) -> RESP {
	return gen_pop(client, args, list_pop_front)
}

rpop :: proc(client: ^Client, args: []string) -> RESP {
	return gen_pop(client, args, list_pop_back)
}

gen_pop :: proc(
	client: ^Client,
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

	popped, err := popper(client.server.database, key, count)
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

blpop :: proc(client: ^Client, args: []string) -> RESP {
	return bpop(client, args, list_pop_front)
}

brpop :: proc(client: ^Client, args: []string) -> RESP {
	return bpop(client, args, list_pop_back)
}

bpop :: proc(
	client: ^Client,
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

		list, _ := list_get(client.server.database, key)
		if list.len > 0 {
			popped, ok := popper(client.server.database, key, 1)
			resp := RESP_Array{}
			append(&resp.elements, RESP_Bulk_String{key})
			append(&resp.elements, RESP_Bulk_String{popped[0]})
			return resp
		}

		time.sleep(100 * time.Millisecond)
	}

	return RESP_Null_Array{}
}

type :: proc(client: ^Client, args: []string) -> RESP {
	type, err := generic_type(client.server.database, args[1])
	if err != nil {
		return RESP_Simple_String{"none"}
	}

	return RESP_Simple_String{type}
}

xadd :: proc(client: ^Client, args: []string) -> RESP {
	field_args := args[3:][:]
	fields := make(map[string]string)
	for i := 1; i < len(field_args); i += 2 {
		key := field_args[i - 1]
		val := field_args[i]
		fields[key] = val
	}

	entry, err := stream_add(client.server.database, args[1], args[2], fields)
	if err != nil {
		return RESP_Simple_Error{error_string(err)}
	}

	if entry == nil {
		return RESP_Null_Bulk_String{}
	}
	return RESP_Bulk_String{stream_entry_id_string(entry.id)}
}

xrange :: proc(client: ^Client, args: []string) -> RESP {
	entries, err := stream_range(client.server.database, args[1], args[2], args[3])
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

maybe_id :: proc(s: string) -> bool {
	n, n_ok := strconv.parse_u64(s)
	if n_ok {
		return true
	}
	if len(strings.split(s, "-")) == 2 {
		return true
	}
	if 0 == strings.compare(s, "$") {
		return true
	}

	return false
}

xread :: proc(client: ^Client, args: []string) -> RESP {
	block_index := 0
	block_found := false
	for arg in args {
		if 0 == strings.compare(strings.to_upper(arg), "BLOCK") {
			block_found = true
			break
		}
		block_index += 1
	}

	streams_index := 0
	streams_found := false
	for arg in args {
		if 0 == strings.compare(strings.to_upper(arg), "STREAMS") {
			streams_found = true
			streams_index += 1
			break
		}
		streams_index += 1
	}

	if !streams_found {
		return RESP_Null_Array{}
	}

	ids_index := streams_index + 1
	for id in args[streams_index + 1:] {
		if maybe_id(id) {
			break
		}
		ids_index += 1
	}

	for i := 0; i < len(args) - ids_index; i += 1 {
		if 0 == strings.compare(args[ids_index], "$") {
			top, top_err := stream_top(client.server.database, args[streams_index + i])
			if top != nil {
				args[ids_index + i] = stream_entry_id_string(top.id)
			} else {
				args[ids_index + i] = stream_entry_id_string(Stream_ID_Zero)
			}
		}
	}

	streams_count := ids_index - streams_index

	result := RESP_Array{}
	result.elements = make(type_of(result.elements))

	block: if block_found {
		block_ms: i64 = 0
		if block_found && block_index + 1 < len(args) {
			ms, ok := strconv.parse_i64(args[block_index + 1])
			if ok {
				block_ms = ms
			}
		}
		block_duration := time.Duration(block_ms * 1_000_000)

		resolution_ms: i64 = 100
		resolution_ns: i64 = resolution_ms * 1_000_000
		sleep_duration := time.Duration(resolution_ns)

		for i: i64 = 0; true; i += resolution_ns {
			if block_duration > 0 && i > i64(block_duration) {
				fmt.printfln("Timeout of [%dms] execeeded", block_ms)
				return RESP_Null_Array{}
			}

			if streams_count > 1 {
				_xread_stream_multi(
					client,
					args[streams_index:ids_index],
					args[ids_index:],
					&result,
				)
			} else {
				_xread_stream_single(client, args[streams_index], args[ids_index], &result)
			}

			if len(result.elements) > 0 {
				break block
			}

			time.sleep(sleep_duration)
		}
	} else {
		if streams_count > 1 {
			_xread_stream_multi(client, args[streams_index:ids_index], args[ids_index:], &result)
		} else {
			_xread_stream_single(client, args[streams_index], args[ids_index], &result)
		}

	}

	if len(result.elements) == 0 {
		return RESP_Null_Array{}
	} else {
		return result
	}
}

_xread_stream_multi :: proc(
	client: ^Client,
	streams: []string,
	ids: []string,
	parent: ^RESP_Array,
) {
	for i := 0; i < len(streams); i += 1 {
		_xread_stream_single(client, streams[i], ids[i], parent)
	}
}

_xread_stream_single :: proc(client: ^Client, stream: string, id: string, parent: ^RESP_Array) {
	stream_arr := RESP_Array{}
	stream_arr.elements = make(type_of(stream_arr.elements))
	append(&stream_arr.elements, RESP_Bulk_String{stream})

	entries, err := stream_read(client.server.database, stream, id)

	if err != nil {
		fmt.printfln("xread failed with error: %v", err)
		return
	}

	if len(entries) == 0 {
		return
	}

	arr_of_entries := RESP_Array{}
	arr_of_entries.elements = make(type_of(arr_of_entries.elements))

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
		append(&arr_of_entries.elements, entry_arr)
	}

	append(&stream_arr.elements, arr_of_entries)
	append(&parent.elements, stream_arr)
}

multi :: proc(client: ^Client, args: []string) -> RESP {
	client.transactions = make(type_of(client.transactions))
	client.state = .Transaction
	return RESP_Simple_String{"OK"}
}

transaction_queue :: proc(client: ^Client, args: []string) -> RESP {
	cmd := get_command(args[0])
	append(&client.transactions, Transaction{cmd, args})
	return RESP_Simple_String{"QUEUED"}
}

exec :: proc(client: ^Client, args: []string) -> RESP {
	if client.state == .Normal {
		return RESP_Simple_Error{"ERR EXEC without MULTI"}
	}

	resp: RESP_Array
	resp.elements = make(type_of(resp.elements))
	for trans in client.transactions {
		r := trans.cmd.handler(client, trans.args)
		append(&resp.elements, r)
	}

	delete(client.transactions)
	client.state = .Normal
	return resp
}

discard :: proc(client: ^Client, args: []string) -> RESP {
	if client.state == .Normal {
		return RESP_Simple_Error{"ERR DISCARD without MULTI"}
	}

	clear(&client.transactions)
	client.state = .Normal
	return RESP_Simple_String{"OK"}
}

info :: proc(client: ^Client, args: []string) -> RESP {
	repl_arg_index := index_string(args, "replication")
	if repl_arg_index > -1 {
		rolestr: string
		switch client.role {
		case .Master:
			rolestr = "master"
		case .Slave:
			rolestr = "slave"
		}

		str := fmt.tprintf(
			"role:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d",
			rolestr,
			"8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
			0,
		)
		return RESP_Bulk_String{str}
	}

	return RESP_Null_Bulk_String{}
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

index_string :: proc(arr: []string, s: string) -> int {
	for item, i in arr {
		if item == s {
			return i
		}
	}
	return -1
}

