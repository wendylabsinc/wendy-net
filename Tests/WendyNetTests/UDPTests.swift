#if WendyNetBackendStandard
import Testing
@testable import WendyNet

/// Test-only holder for mutable state captured into @Sendable closures.
private final class TestBox<T>: @unchecked Sendable {
    var value: T
    init(_ value: T) { self.value = value }
}

// MARK: - Named helpers
//
// Swift 6.3.1 has a SIL crash (LifetimeDependenceDiagnostics) when generating
// thunks for nested typed-throws closures captured into Task initialisers.
// Extracting the closure bodies into named async functions sidesteps the bug.

private func runUDPEchoServer(listener: Listener<ByteBuffer>, captured: TestBox<[UInt8]>) async throws(WendyNetError) {
    try await listener.executeThenClose { accepted throws(WendyNetError) in
        guard let serverChannel = try await accepted.next() else { return }
        try await echoOnce(channel: serverChannel, captured: captured)
    }
}

private func echoOnce(channel: Channel<ByteBuffer>, captured: TestBox<[UInt8]>) async throws(WendyNetError) {
    try await channel.executeThenClose { inbound, outbound throws(WendyNetError) in
        guard var msg = try await inbound.next() else { return }
        let bytes = msg.readBytes(length: msg.readableBytes) ?? []
        captured.value = bytes
        _ = try await outbound.write(ByteBuffer(bytes: bytes))
    }
}

private func sendAndReceive(client: Channel<ByteBuffer>, payload: [UInt8], response: TestBox<[UInt8]>) async throws(WendyNetError) {
    try await client.executeThenClose { inbound, outbound throws(WendyNetError) in
        _ = try await outbound.write(ByteBuffer(bytes: payload))
        if var reply = try await inbound.next() {
            response.value = reply.readBytes(length: reply.readableBytes) ?? []
        }
    }
}

private func acceptTwoAndCollect(listener: Listener<ByteBuffer>, seen: TestBox<[String]>) async throws(WendyNetError) {
    try await listener.executeThenClose { accepted throws(WendyNetError) in
        for _ in 0 ..< 2 {
            guard let channel = try await accepted.next() else { break }
            try await collectOne(channel: channel, seen: seen)
        }
    }
}

private func collectOne(channel: Channel<ByteBuffer>, seen: TestBox<[String]>) async throws(WendyNetError) {
    try await channel.executeThenClose { inbound, _ throws(WendyNetError) in
        guard var msg = try await inbound.next() else { return }
        let bytes = msg.readBytes(length: msg.readableBytes) ?? []
        seen.value.append(String(decoding: bytes, as: UTF8.self))
    }
}

private func sendOne(client: Channel<ByteBuffer>, text: String) async throws(WendyNetError) {
    try await client.executeThenClose { _, outbound throws(WendyNetError) in
        _ = try await outbound.write(ByteBuffer(bytes: Array(text.utf8)))
    }
}

// MARK: - Tests

@Test
func udpRoundTrip() async throws {
    let net = WendyNet()

    let listener = try await ServerBootstrap(wendyNet: net)
        .reliability(.unreliable)
        .security(.insecure)
        .bind(port: 0)

    let serverCaptured = TestBox<[UInt8]>([])
    let serverTask = Task {
        try? await runUDPEchoServer(listener: listener, captured: serverCaptured)
    }

    let client = try await ClientBootstrap(wendyNet: net)
        .reliability(.unreliable)
        .security(.insecure)
        .connect(to: Endpoint(hostname: "127.0.0.1", port: listener.port))

    #expect(client.transport.kind == .udp)
    #expect(client.transport.isDatagramOriented)

    let payload: [UInt8] = Array("ping".utf8)
    let responseBox = TestBox<[UInt8]>([])
    try await sendAndReceive(client: client, payload: payload, response: responseBox)

    _ = await serverTask.value

    #expect(serverCaptured.value == payload, "server received bytes did not match what client sent")
    #expect(responseBox.value == payload, "client did not receive the echoed payload")
}

@Test
func udpServerSeesMultiplePeersAsDistinctChannels() async throws {
    let net = WendyNet()

    let listener = try await ServerBootstrap(wendyNet: net)
        .reliability(.unreliable)
        .bind(port: 0)

    let seen = TestBox<[String]>([])
    let serverTask = Task {
        try? await acceptTwoAndCollect(listener: listener, seen: seen)
    }

    let clientA = try await ClientBootstrap(wendyNet: net)
        .reliability(.unreliable)
        .connect(to: Endpoint(hostname: "127.0.0.1", port: listener.port))
    let clientB = try await ClientBootstrap(wendyNet: net)
        .reliability(.unreliable)
        .connect(to: Endpoint(hostname: "127.0.0.1", port: listener.port))

    try await sendOne(client: clientA, text: "from-a")
    try await sendOne(client: clientB, text: "from-b")

    _ = await serverTask.value

    #expect(seen.value.sorted() == ["from-a", "from-b"])
}
#endif
