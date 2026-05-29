#ifndef CWENDYNET_H
#define CWENDYNET_H

/* WendyNet async TCP host imports.
 *
 * Imports under module "wendy" are merged at link time with declarations
 * from CWendyLite (and any other host-import target) into a single WASM
 * import section, so it does not matter that this set is split across
 * Swift packages. The host runtime is the ABI authority. */

#define WENDYNET_EVENT_ACCEPT_READY 1
#define WENDYNET_EVENT_READ_READY   2
#define WENDYNET_EVENT_WRITE_READY  4
#define WENDYNET_EVENT_CLOSED       8
#define WENDYNET_EVENT_ERROR        16

#define WENDYNET_STATUS_READABLE 1
#define WENDYNET_STATUS_WRITABLE 2
#define WENDYNET_STATUS_CLOSED   4
#define WENDYNET_STATUS_ERROR    8

__attribute__((import_module("wendy"), import_name("wendynet_init")))
int wendynet_init(int handler_id);

__attribute__((import_module("wendy"), import_name("wendynet_drain_events")))
int wendynet_drain_events(void);

__attribute__((import_module("wendy"), import_name("wendynet_tcp_listen")))
int wendynet_tcp_listen(int port, int backlog);

__attribute__((import_module("wendy"), import_name("wendynet_tcp_connect")))
int wendynet_tcp_connect(const char *host, int host_len, int port);

__attribute__((import_module("wendy"), import_name("wendynet_listener_accept")))
int wendynet_listener_accept(int listener_handle);

__attribute__((import_module("wendy"), import_name("wendynet_listener_close")))
int wendynet_listener_close(int listener_handle);

/* Returns the local port bound to this listener (useful when the caller
 * passed port 0 and the OS assigned an ephemeral one). -1 on error. */
__attribute__((import_module("wendy"), import_name("wendynet_listener_port")))
int wendynet_listener_port(int listener_handle);

__attribute__((import_module("wendy"), import_name("wendynet_socket_status")))
int wendynet_socket_status(int socket_handle);

__attribute__((import_module("wendy"), import_name("wendynet_socket_recv")))
int wendynet_socket_recv(int socket_handle, char *buf, int len);

__attribute__((import_module("wendy"), import_name("wendynet_socket_send")))
int wendynet_socket_send(int socket_handle, const char *data, int len);

__attribute__((import_module("wendy"), import_name("wendynet_socket_close")))
int wendynet_socket_close(int socket_handle);

#endif
