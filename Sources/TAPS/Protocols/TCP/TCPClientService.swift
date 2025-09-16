/// TCP service implementing ClientServiceProtocol
public struct TCPClientService<
  InboundMessage: Sendable,
  OutboundMessage: Sendable
>: ClientServiceProtocol {
  public typealias Parameters = TCPClientParameters
  public typealias Client = TCPSocket<InboundMessage, OutboundMessage>

  let resolve: @Sendable (TAPSContext) async throws -> (host: String, port: Int)
  private let protocolStack:
    ProtocolStack<_NetworkInputBytes, InboundMessage, OutboundMessage, _NetworkOutputBytes>

  internal init(
    protocolStack: ProtocolStack<
      NetworkInputBytes, InboundMessage, OutboundMessage, NetworkOutputBytes
    >,
    resolve: @escaping @Sendable (TAPSContext) async throws -> (host: String, port: Int)
  ) {
    self.resolve = resolve
    self.protocolStack = ProtocolStack.unverified {
      [NetworkBytesDuplexHandler()] + protocolStack.handlers()
    }
  }

  /// Create TCP client with given parameters
  public func withConnection<T: Sendable>(
    parameters: Parameters,
    context: TAPSContext,
    perform: @escaping @Sendable (Client) async throws -> T
  ) async throws -> T {
    let (host, port) = try await resolve(context)
    return try await Client.withClientConnection(
      host: host,
      port: port,
      parameters: parameters,
      context: context,
      protocolStack: protocolStack,
      perform: perform
    )
  }
}

extension ClientServiceProtocol
where Self == TCPClientService<NetworkInputBytes, NetworkOutputBytes> {
  public static func tcp(host: String, port: Int) -> TCPClientService<
    NetworkInputBytes, NetworkOutputBytes
  > {
    TCPClientService(protocolStack: .init()) { _ in
      return (host, port)
    }
  }

  public static func tcp<
    Reference: Sendable,
    Peer: InternetHost
  >(
    to reference: Reference,
    resolver: PeerDiscoveryMechanism<some PeerDiscoveryMechanismProtocol<Reference, Peer>>
  ) -> TCPClientService<NetworkInputBytes, NetworkOutputBytes> {
    TCPClientService(protocolStack: .init()) { context in
      try await resolver.withMechanism(forContext: context) { resolver in
        let hosts = try await resolver.discover(reference)
        guard let host = hosts.first else {
          throw PeerDiscoveryError.cannotResolve()
        }

        return (host.hostname, host.port)
      }
    }
  }
}
