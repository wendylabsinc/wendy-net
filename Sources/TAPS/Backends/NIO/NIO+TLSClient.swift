#if canImport(NIOPosix)
  internal import NIOCore
  internal import NIOSSL
  internal import NIOPosix
  import Logging
  import ServiceLifecycle

  extension NIOSSLHandler {
    internal static func crashOnMisconfiguration(
      context: NIOSSLContext,
      serverHostname: String?,
      customVerificationCallback: NIOSSLCustomVerificationCallback? = nil,
      configuration: Configuration
    ) -> NIOSSLClientHandler {
      do {
        return try NIOSSLClientHandler(
          context: context,
          serverHostname: serverHostname,
          configuration: NIOSSLHandler.Configuration()
        )
      } catch {
        preconditionFailure("Invalid NIOSSL configuration")
      }
    }
  }

  extension ConnectionSubprotocol<
    ByteBuffer,
    ByteBuffer,
    ByteBuffer,
    IOData
  > {
    static func tls(
      configuration: TLSConfiguration,
      serverHostname: String?
    ) throws -> Self {
      let context = try NIOSSLContext(configuration: configuration)
      return Self {
        NIOSSLClientHandler.crashOnMisconfiguration(
          context: context,
          serverHostname: serverHostname,
          customVerificationCallback: nil,
          configuration: NIOSSLHandler.Configuration()
        )
      }
    }
  }

  /// TCP Client as proper with real SwiftNIO implementation
  @available(macOS 15.0, *)
  public actor TLSClient<
    InboundMessage: Sendable,
    OutboundMessage: Sendable
  >: DuplexClientProtocol {
    // TODO: Fix up for embedded
    public typealias ConnectionError = any Error

    // Actor-isolated state
    private nonisolated let _inbound: NIOAsyncChannelInboundStream<InboundMessage>
    private nonisolated let outbound: NIOAsyncChannelOutboundWriter<OutboundMessage>
    private nonisolated let logger = Logger(label: "engineer.edge.taps.tls")

    public nonisolated var inbound: some AsyncSequence<InboundMessage, ConnectionError> {
      _inbound
    }

    internal init(
      socket: TCPSocket<InboundMessage, OutboundMessage>
    ) {
      self._inbound = socket._inbound
      self.outbound = socket.outbound
    }

    public func run() async throws {
      try await gracefulShutdown()
    }

    internal static func withConnection<T: Sendable>(
      host: String,
      port: Int,
      parameters: TLSClientParameters.TCP,
      context: TAPSContext,
      protocolStack: ProtocolStack<
        _NetworkInputBytes, InboundMessage, OutboundMessage, _NetworkBytes
      > = ProtocolStack(),
      perform: @escaping @Sendable (TLSClient) async throws -> T
    ) async throws -> T {
      let tlsSubprotocol = try ConnectionSubprotocol.tls(
        // TODO: Make configurable
        configuration: .clientDefault,
        serverHostname: host
      )
      return try await TCPSocket<
        InboundMessage,
        OutboundMessage
      >.withClientConnection(
        host: host,
        port: port,
        parameters: parameters.tcp,
        context: context,
        protocolStack: ProtocolStack {
          tlsSubprotocol
          protocolStack
        }
      ) { socket in
        return try await perform(TLSClient(socket: socket))
      }
    }
  }
#endif
