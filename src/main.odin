package main

import "redis"

import "core:fmt"
import "core:os"
import "core:strconv"
import "core:strings"

main :: proc() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.eprintln("Logs from your program will appear here!")

	ip := "127.0.0.1"
	port := 6379
	role: redis.Client_Role = .Master
	argc := len(os.args)
	if argc > 1 {
		port_index := index_string(os.args, "--port")
		if port_index > -1 && port_index + 1 < argc {
			_port, ok := strconv.parse_int(os.args[port_index + 1])
			if ok {
				port = _port
			}
		}
		replicaof_index := index_string(os.args, "--replicaof")
		if replicaof_index > -1 && replicaof_index + 1 < argc {
			addr := os.args[replicaof_index + 1]
			parts := strings.split(addr, " ")
			// ip = parts[0]
			// port, _ = strconv.parse_int(parts[1])
			role = .Slave
		}
	}

	redis.connect("127.0.0.1", port, role)
}

index_string :: proc(arr: []string, s: string) -> int {
	for item, i in arr {
		if item == s {
			return i
		}
	}
	return -1
}

