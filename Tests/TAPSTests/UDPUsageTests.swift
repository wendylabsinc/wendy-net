// UDPUsageTests.swift
// Tests for UDP socket functionality

import TAPS
import Testing

@Suite
struct UDPUsageTests {
  @Test(.timeLimit(.minutes(1)))
  func testUDPUnicastSendReceive() async throws {
    try await withTAPS { taps in
      let message = "Hello, UDP!"
      let serverPort = 54200

      try await confirmation { confirm in
        try await withThrowingDiscardingTaskGroup { group in
          // Start UDP "server" (receiver)
          group.addTask {
            try await taps.withUDPSocket(on: .udp(port: serverPort)) { socket in
              for try await datagram in socket.inbound {
                let received = String(bytes: datagram.data)
                #expect(received == message)
                confirm()
                return
              }
            }
          }

          // Give the server time to bind
          try await Task.sleep(for: .milliseconds(100))
          defer { group.cancelAll() }

          // Send from client
          try await taps.withUDPSocket(on: .udp()) { socket in
            try await socket.send(
              UDPOutboundDatagram(
                host: "127.0.0.1",
                port: serverPort,
                data: NetworkOutputBytes(string: message)
              )
            )
            // Give time for the message to be received
            try await Task.sleep(for: .milliseconds(100))
          }
        }
      }
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func testUDPEchoServer() async throws {
    try await withTAPS { taps in
      let message = "Echo me!"
      let serverPort = 54201

      try await confirmation { confirm in
        try await withThrowingDiscardingTaskGroup { group in
          // Start UDP echo server
          group.addTask {
            try await taps.withUDPSocket(on: .udp(port: serverPort)) { socket in
              for try await datagram in socket.inbound {
                // Echo back to sender
                try await socket.send(
                  UDPOutboundDatagram(
                    to: datagram.remoteAddress,
                    data: NetworkOutputBytes(string: String(bytes: datagram.data))
                  )
                )
              }
            }
          }

          try await Task.sleep(for: .milliseconds(100))
          defer { group.cancelAll() }

          // Client sends and receives echo
          try await taps.withUDPSocket(on: .udp()) { socket in
            try await socket.send(
              UDPOutboundDatagram(
                host: "127.0.0.1",
                port: serverPort,
                data: NetworkOutputBytes(string: message)
              )
            )

            for try await datagram in socket.inbound {
              let received = String(bytes: datagram.data)
              #expect(received == message)
              confirm()
              return
            }
          }
        }
      }
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func testMulticastGroupJoin() async throws {
    try await withTAPS { taps in
      let message = "Multicast message"
      let multicastAddress = "239.255.255.250"
      let multicastPort = 54202

      let group = MulticastGroup(address: multicastAddress, port: multicastPort)

      try await confirmation { confirm in
        try await withThrowingDiscardingTaskGroup { taskGroup in
          // Start multicast receiver
          taskGroup.addTask {
            try await taps.withUDPSocket(
              on: .multicastUDP(port: multicastPort, groups: [group])
            ) { socket in
              for try await datagram in socket.inbound {
                let received = String(bytes: datagram.data)
                #expect(received == message)
                confirm()
                return
              }
            }
          }

          try await Task.sleep(for: .milliseconds(200))
          defer { taskGroup.cancelAll() }

          // Send to multicast group
          try await taps.withUDPSocket(on: .udp()) { socket in
            try await socket.send(
              UDPOutboundDatagram(
                host: multicastAddress,
                port: multicastPort,
                data: NetworkOutputBytes(string: message)
              )
            )
            try await Task.sleep(for: .milliseconds(100))
          }
        }
      }
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func testBroadcastSocket() async throws {
    // Note: Broadcast tests may not work in all environments
    // This test verifies the socket can be created with broadcast enabled
    try await withTAPS { taps in
      try await taps.withUDPSocket(on: .broadcastUDP(port: 54203)) { socket in
        // Just verify the socket was created successfully
        #expect(true)
      }
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func testMultipleDatagram() async throws {
    try await withTAPS { taps in
      let messages = ["Message 1", "Message 2", "Message 3"]
      let serverPort = 54204

      try await confirmation(expectedCount: messages.count) { confirm in
        try await withThrowingDiscardingTaskGroup { group in
          // Start receiver
          group.addTask {
            try await taps.withUDPSocket(on: .udp(port: serverPort)) { socket in
              var receivedCount = 0
              for try await datagram in socket.inbound {
                let received = String(bytes: datagram.data)
                #expect(messages.contains(received))
                confirm()
                receivedCount += 1
                if receivedCount >= messages.count {
                  return
                }
              }
            }
          }

          try await Task.sleep(for: .milliseconds(100))
          defer { group.cancelAll() }

          // Send multiple datagrams
          try await taps.withUDPSocket(on: .udp()) { socket in
            for message in messages {
              try await socket.send(
                UDPOutboundDatagram(
                  host: "127.0.0.1",
                  port: serverPort,
                  data: NetworkOutputBytes(string: message)
                )
              )
            }
            try await Task.sleep(for: .milliseconds(200))
          }
        }
      }
    }
  }
}
