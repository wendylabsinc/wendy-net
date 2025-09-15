public struct TCPServerService<
    InboundMessage: Sendable,
    OutboundMessage: Sendable
>: ServerServiceProtocol {
    public typealias Parameters = TCPServerParameters
    public typealias Server = TCPServer
    
    private let host: String
    private let port: Int
    private let protocolStack: ProtocolStack<_NetworkInputBytes, InboundMessage, OutboundMessage, _NetworkOutputBytes>
    
    public init(host: String, port: Int) where InboundMessage == NetworkInputBytes, OutboundMessage == NetworkOutputBytes {
        self.host = host
        self.port = port
        self.protocolStack = ProtocolStack {
            NetworkBytesDuplexHandler()
        }
    }
    
    public init(
        host: String,
        port: Int,
        protocolStack: ProtocolStack<NetworkInputBytes, InboundMessage, OutboundMessage, NetworkOutputBytes>
    ) {
        self.host = host
        self.port = port
        self.protocolStack = ProtocolStack.unverified {
            [NetworkBytesDuplexHandler()] + protocolStack.handlers()
        }
    }
    
    public func withServer<T: Sendable>(
        parameters: TCPServerParameters,
        context: TAPSContext,
        perform: @Sendable @escaping (TCPServer<InboundMessage, OutboundMessage>) async throws -> T
    ) async throws -> T {
        return try await TCPServer.withServer(
            host: host,
            port: port,
            parameters: parameters,
            context: context,
            protocolStack: protocolStack,
            perform: perform
        )
    }
}

extension ServerServiceProtocol where Self == TCPServerService<NetworkInputBytes, NetworkOutputBytes> {
    public static func tcp(
        host: String = "0.0.0.0",
        port: Int
    ) -> TCPServerService<NetworkInputBytes, NetworkOutputBytes> {
        TCPServerService(host: host, port: port)
    }
}
