package main

import "core:fmt"
import "core:net"
import "core:strings"
import "core:bytes"
import "core:os"
import "redis"

main :: proc (){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    fmt.eprintln("Logs from your program will appear here!")

    redis.server("127.0.0.1", 6379)
}
