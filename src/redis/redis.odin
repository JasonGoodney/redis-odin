package redis

import "core:fmt"
import "core:net"
import "core:strings"
import "core:thread"

server :: proc(ip: string, port: int) {
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

handle_msg :: proc(sock: net.TCP_Socket) {
	buffer: [256]u8
	for {
		bytes_recv, err_recv := net.recv_tcp(sock, buffer[:])
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

		cmd := strings.clone_from_bytes(received)

		token, _, err := parse(received, 0)

		response: string
		if strings.contains(cmd, "PING") {
			response = cmd_ping()
		} else {
			#partial switch tok in token {
			case Array_Token:
				#partial switch val in tok.value[0] {
				case String_Token:
					if 0 == strings.compare(val.value, "ECHO") {
						response = cmd_echo(tok)
					}
				}

			}
		}


		buffer := transmute([]u8)response
		bytes_sent, err_send := net.send_tcp(sock, buffer)
		if err_send != nil {
			fmt.println("Failed to send data")
		}

		sent := buffer[:bytes_sent]
		fmt.printfln("Server send [ %d bytes ]: %s", len(sent), sent)
	}
	net.close(sock)
}

cmd_ping :: proc() -> string {
	return "+PONG\r\n"
}

cmd_echo :: proc(token: Array_Token) -> string {
	sb := strings.builder_make()
	strings.write_rune(&sb, '$')

	#partial switch val in token.value[1] {
	case String_Token:
		strings.write_int(&sb, len(val.value))
		strings.write_string(&sb, "\r\n")
		strings.write_string(&sb, val.value)
		strings.write_string(&sb, "\r\n")
		return strings.to_string(sb)
	}

	return ""
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
