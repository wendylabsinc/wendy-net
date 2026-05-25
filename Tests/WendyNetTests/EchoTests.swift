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

private func runEchoServer(listener: Listener<ByteBuffer>, captured: TestBox<[UInt8]>) async throws(WendyNetError) {
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

@Test
func clientServerRoundTrip() async throws {
    let port: UInt16 = 28443
    let net = WendyNet()

    let listener = try await ServerBootstrap(wendyNet: net)
        .security(.insecure)
        .bind(port: port)

    let serverCaptured = TestBox<[UInt8]>([])
    let serverTask = Task {
        try? await runEchoServer(listener: listener, captured: serverCaptured)
    }

    let client = try await ClientBootstrap(wendyNet: net)
        .security(.insecure)
        .connect(to: Endpoint(hostname: "127.0.0.1", port: port))

    let payload: [UInt8] = Array("ping".utf8)
    let responseBox = TestBox<[UInt8]>([])
    try await sendAndReceive(client: client, payload: payload, response: responseBox)

    _ = await serverTask.value

    #expect(serverCaptured.value == payload, "server received bytes did not match what client sent")
    #expect(responseBox.value == payload, "client did not receive the echoed payload")
}

#endif
