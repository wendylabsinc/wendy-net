#if canImport(NIOPosix)
  import AsyncAlgorithms
  internal import NIOCore
  internal import NIOPosix
  import Logging
  import ServiceLifecycle

  /// TCP Client as proper with real SwiftNIO implementation
  @available(macOS 15.0, *)
  public struct TCPSocket<
    InboundMessage: Sendable,
    OutboundMessage: Sendable
  >: DuplexClientProtocol {
    public typealias ConnectionError = any Error

    // Actor-isolated state
    internal nonisolated let _inbound: NIOAsyncChannelInboundStream<InboundMessage>
    internal nonisolated let outbound: NIOAsyncChannelOutboundWriter<OutboundMessage>
    private nonisolated let logger = Logger(label: "engineer.edge.taps.tcp")

    public nonisolated var inbound: some AsyncSequence<InboundMessage, ConnectionError> {
      _inbound
    }

    /// Initialize TCP client with endpoint and parameters
    internal init(
      inbound: NIOAsyncChannelInboundStream<InboundMessage>,
      outbound: NIOAsyncChannelOutboundWriter<OutboundMessage>
    ) {
      self._inbound = inbound
      self.outbound = outbound
    }

    public func run() async throws {
      try await gracefulShutdown()
    }

    internal static func withClientConnection<T: Sendable>(
      host: String,
      port: Int,
      parameters: TCPClientParameters,
      context: TAPSContext,
      protocolStack: ProtocolStack<ByteBuffer, InboundMessage, OutboundMessage, IOData> =
        ProtocolStack(),
      perform: @escaping @Sendable (TCPSocket<InboundMessage, OutboundMessage>) async throws -> T
    ) async throws -> T {
      // Bootstrap TCP connection with simpler pipeline
      let channel = try await ClientBootstrap(group: .singletonMultiThreadedEventLoopGroup)
        .applyParameters(parameters)
        .channelInitializer { [protocolStack] channel in
          do {
            try channel.pipeline.syncOperations.addHandlers(protocolStack.handlers())
            return channel.eventLoop.makeSucceededVoidFuture()
          } catch {
            return channel.eventLoop.makeFailedFuture(error)
          }
        }
        .connect(host: host, port: port)
        .flatMapThrowing { channel in
          try NIOAsyncChannel<InboundMessage, OutboundMessage>(
            wrappingChannelSynchronously: channel)
        }
        .get()

      return try await channel.executeThenClose {
        inbound,
        outbound in
        return try await perform(
          TCPSocket(
            inbound: inbound,
            outbound: outbound
          )
        )
      }
    }

    public func send(_ message: OutboundMessage) async throws {
      try await outbound.write(message)
    }
  }
#endif
