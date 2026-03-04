package main

import "core:fmt"
import "core:os"
import "core:strconv"
import "redis"

main :: proc() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.eprintln("Logs from your program will appear here!")

	port := 6379
	argc := len(os.args)
	if argc > 1 {
		port_index := index_string(os.args, "--port")
		if port_index > -1 && port_index + 1 < argc {
			_port, ok := strconv.parse_int(os.args[2])
			if ok {
				port = _port
			}
		}
	}

	redis.connect("127.0.0.1", port)
}

index_string :: proc(arr: []string, s: string) -> int {
	for item, i in arr {
		if item == s {
			return i
		}
	}
	return -1
}

