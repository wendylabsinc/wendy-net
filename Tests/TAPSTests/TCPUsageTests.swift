// TCPTestExample.swift
// Example for testing TCP connection with real servers

import HTTPTypes
import TAPS
import Testing

/// Example usage of TAPS TCP client
@Suite
struct TCPUsageTests {
  @Test(.timeLimit(.minutes(1)))
  public func testLocalTCPConnection() async throws {
    try await withTAPS { taps in
      let message = "Hello, server!"

      try await confirmation { confirm in
        try await withThrowingDiscardingTaskGroup { group in
          group.addTask {
            try await taps.withServer(
              on: .tcp(host: "127.0.0.1", port: 54_123)
            ) { inboundClient in
              for try await chunk in inboundClient.inbound {
                let chunk = String(bytes: chunk)
                #expect(chunk == message)
                confirm()
                return
              }
            }
          }

          try await Task.sleep(for: .milliseconds(250))
          defer { group.cancelAll() }

          try await taps.withConnection(
            target: .tcp(host: "127.0.0.1", port: 54_123)
          ) { client in
            try await client.send(NetworkOutputBytes(string: message))

            for try await _ in client.inbound {
              #expect(Bool(false), "Expected no reply")
            }
          }
        }
      }
    }
  }
}

@Suite
struct HTTP1ClientTests {
  @Test func testHTTP1Client() async throws {
    try await withTAPS { taps in
      try await taps.withConnection(
        target: .http1(host: "example.com")
      ) { client in
        try await client.withResponse(
          to: HTTPRequest(
            method: .get,
            scheme: "http",
            authority: "example.com",
            path: "/"
          )
        ) { response in
          print(response.head)
          for try await part in response.body {
            print(String(bytes: part))
          }
        }
      }
    }
  }

  @Test func testHTTPS1Client() async throws {
    try await withTAPS { taps in
      try await taps.withConnection(
        target: .https1(host: "swiftonserver.com")
      ) { client in
        try await client.withResponse(
          to: HTTPRequest(
            method: .get,
            scheme: "https",
            authority: "swiftonserver.com",
            path: "/"
          )
        ) { response in
          print(response.head)
          for try await part in response.body {
            print(String(bytes: part))
          }
        }
      }
    }
  }
}

func withTAPS(
  _ perform: @Sendable @escaping (TAPS) async throws -> Void
) async throws {
  let taps = try await TAPS()

  do {
    try await withThrowingDiscardingTaskGroup { group in
      // Run TAPS service
      group.addTask {
        try await taps.run()
      }

      defer { group.cancelAll() }
      try await perform(taps)
    }
  } catch is CancellationError {
    // Expected
  }
}
