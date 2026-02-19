package redis

import "core:fmt"
import "core:net"
import "core:strconv"
import "core:strings"
import "core:thread"

Set_Option :: union {
	Set_Option_PX,
}

Set_Option_PX :: struct {
	expire_milliseconds: i64,
}

connect :: proc(ip: string, port: int) {
	cache_init()

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
		fmt.println("Failed to listen on TCP")
		return
	}

	fmt.printfln("Listening on TCP: %s", net.endpoint_to_string(endpoint))
	for {
		cli, _, err_accept := net.accept_tcp(sock)
		if err_accept != nil {
			fmt.println("Failed to accept TCP connection")
			continue
		}

		thread.create_and_start_with_poly_data(cli, handle_msg)
	}

	net.close(sock)
}

handle_msg :: proc(socket: net.TCP_Socket) {
	buffer: [256]u8
	for {
		bytes_recv, err_recv := net.recv_tcp(socket, buffer[:])
		if err_recv != nil {
			fmt.println("Failed to receive data")
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

		str, err := decode(received)
		array := str.(RESP_Array)

		response := string{}

		cmd := array.elements[0].(RESP_Bulk_String)
		if insensitive_compare(cmd.value, "PING") {
			response = cmd_ping()
		} else if insensitive_compare(cmd.value, "ECHO") {
			response = cmd_echo(array)
		} else if insensitive_compare(cmd.value, "SET") {
			response = cmd_set(array)
		} else if insensitive_compare(cmd.value, "GET") {
			response = cmd_get(array)
		}

		assert(response != "")

		buffer := transmute([]u8)response
		bytes_sent, err_send := net.send_tcp(socket, buffer)
		if err_send != nil {
			fmt.println("Failed to send data")
		}

		sent := buffer[:bytes_sent]
		fmt.printfln("Server sent [ %d bytes ]: %s", len(sent), sent)
	}

	net.close(socket)
}

cmd_ping :: proc() -> string {
	return encode(RESP_Simple_String{"PONG"})
}

cmd_echo :: proc(reply: RESP_Array) -> string {
	return encode(reply.elements[1])
}

cmd_set :: proc(array: RESP_Array) -> string {
	opts := make([dynamic]Set_Option)

	key_tok := array.elements[1].(RESP_Bulk_String)
	val_tok := array.elements[2].(RESP_Bulk_String)

	i := 3
	for i < len(array.elements) {
		opt_tok := array.elements[i].(RESP_Bulk_String)
		opt := strings.to_upper(opt_tok.value)
		switch opt {
		case "PX":
			i += 1
			if i > len(array.elements) {
				continue
			}
			val_tok := array.elements[i].(RESP_Bulk_String)
			expire, ok := strconv.parse_i64(val_tok.value)
			if ok && expire > 0 {
				append(&opts, Set_Option_PX{expire})
			}
		case:
			break
		}

		i += 1
	}

	ok := cache_set(key_tok.value, val_tok.value, opts[:])
	if !ok {
		return encode(RESP_Simple_Error{"ERROR"})
	}

	return encode(RESP_Simple_String{"OK"})
}

cmd_get :: proc(array: RESP_Array) -> string {
	key_tok := array.elements[1].(RESP_Bulk_String)
	val, ok := cache_get(key_tok.value)
	if !ok {
		return encode(RESP_Null_Bulk_String{})
	}

	return encode(RESP_Bulk_String{val})
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
