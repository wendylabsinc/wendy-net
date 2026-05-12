#if WendyNetBackendStandard
import Testing
@testable import WendyNet

@Test
func clientServerRoundTrip() async throws {
    let port: UInt16 = 28443
    let net = WendyNet()

    let listener = try await ServerBootstrap(wendyNet: net)
        .security(.insecure)
        .bind(port: port)

    let serverTask = Task { () -> [UInt8] in
        guard let serverChannel = try await listener.accept() else {
            return []
        }
        guard var inbound = try await serverChannel.receive() else {
            return []
        }
        let received = inbound.readBytes(length: inbound.readableBytes) ?? []
        // Echo back
        _ = try await serverChannel.send(ByteBuffer(bytes: received))
        await serverChannel.close()
        return received
    }

    let client = try await ClientBootstrap(wendyNet: net)
        .security(.insecure)
        .connect(to: Endpoint(hostname: "127.0.0.1", port: port))

    let payload: [UInt8] = Array("ping".utf8)
    _ = try await client.send(ByteBuffer(bytes: payload))

    let response = try await client.receive()
    guard var responseBuf = response else {
        Issue.record("client received nil before payload arrived")
        await client.close()
        await listener.close()
        return
    }
    let responseBytes = responseBuf.readBytes(length: responseBuf.readableBytes) ?? []

    let serverSawBytes = try await serverTask.value

    #expect(serverSawBytes == payload, "server received bytes did not match what client sent")
    #expect(responseBytes == payload, "client did not receive the echoed payload")

    await client.close()
    await listener.close()
}
#endif
