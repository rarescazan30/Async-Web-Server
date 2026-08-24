# Asynchronous Web Server

## Overview
This project is a high-performance, single-threaded asynchronous web server written in C. Designed to handle multiple concurrent client connections efficiently, the server bypasses the traditional thread-per-connection model. Instead, it utilizes an event-driven architecture powered by the Linux `epoll` API to manage non-blocking socket I/O and multiplexing.

## Key Features
*   **Event-Driven Architecture:** Uses `epoll` for efficient I/O multiplexing, allowing the server to handle multiple simultaneous connections on a single thread without blocking.
*   **Non-Blocking I/O:** All socket operations (accepting connections, receiving requests, sending responses) are strictly non-blocking.
*   **State Machine Connection Handling:** Connection lifecycles are managed via an internal state machine, seamlessly transitioning between receiving data, parsing HTTP headers, reading files, and transmitting responses.
*   **Zero-Copy Data Transfer:** Utilizes `sendfile` (where applicable) to transfer static files directly from the disk to the socket buffer, minimizing CPU context switches and memory copying.
*   **Robust HTTP Parsing:** Integrates a robust, C-based HTTP parser to securely and accurately interpret incoming client requests.

## Architecture and Tech Stack
*   **Language:** C
*   **Core APIs:** POSIX Sockets, `epoll`, Linux System Calls (`sendfile`, `fcntl`)
*   **External Libraries:** `http-parser` (for parsing HTTP requests)

## Getting Started

### Prerequisites
*   Linux environment (due to `epoll` and `sendfile` dependencies)
*   GCC or Clang compiler
*   Make

### Building the Server
Navigate to the source directory and compile the project using the provided Makefile:

```bash
cd src
make
```

### Running the Server
Once compiled, you can start the server. Depending on your specific implementation, you may need to specify the port and the directory containing the static files you want to serve. For example:

```bash
./aws --port 8080 --docroot /path/to/static/files
```

### Usage Example
With the server running, you can test it by opening a web browser or using `curl` from another terminal window to request a file:

```bash
curl -i http://localhost:8080/index.html
```

### Benchmarking
To verify the high-concurrency capabilities of the event-driven architecture, the server can be benchmarked using tools like Apache Bench (`ab`) or `wrk`. For example, to test with 10,000 requests across 100 concurrent connections:

```bash
ab -n 10000 -c 100 http://localhost:8080/index.html
```
