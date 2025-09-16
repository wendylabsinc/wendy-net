#if canImport(NIOPosix)
  import AsyncAlgorithms
  internal import NIOCore
  internal import NIOPosix
  import Logging

  public struct TCPServer<
    InboundMessage: Sendable,
    OutboundMessage: Sendable
  >: DuplexServerProtocol {
    public typealias Client = TCPSocket<InboundMessage, OutboundMessage>
    public typealias ConnectionError = any Error

    private nonisolated let inbound:
      NIOAsyncChannelInboundStream<NIOAsyncChannel<InboundMessage, OutboundMessage>>

    private init(
      inbound: NIOAsyncChannelInboundStream<NIOAsyncChannel<InboundMessage, OutboundMessage>>
    ) {
      self.inbound = inbound
    }

    internal static func withServer<T: Sendable>(
      host: String,
      port: Int,
      parameters: TCPServerParameters,
      context: TAPSContext,
      protocolStack: ProtocolStack<
        _NetworkInputBytes, InboundMessage, OutboundMessage, _NetworkOutputBytes
      > = ProtocolStack(),
      perform: @escaping @Sendable (TCPServer) async throws -> T
    ) async throws -> T {
      let server = try await ServerBootstrap(group: .singletonMultiThreadedEventLoopGroup)
        .serverChannelOption(.backlog, value: parameters.backlog)
        .serverChannelOption(.socketOption(.so_reuseaddr), value: 1)
        .childChannelOption(.socketOption(.so_reuseaddr), value: 1)
        .childChannelOption(.maxMessagesPerRead, value: 1)
        .childChannelInitializer { channel in
          do {
            try channel.pipeline.syncOperations.addHandlers(protocolStack.handlers())
            return channel.eventLoop.makeSucceededVoidFuture()
          } catch {
            return channel.eventLoop.makeFailedFuture(error)
          }
        }
        .bind(host: host, port: port) { client in
          return client.eventLoop.submit {
            try NIOAsyncChannel<
              InboundMessage,
              OutboundMessage
            >(wrappingChannelSynchronously: client)
          }
        }

      return try await server.executeThenClose { inbound in
        return try await perform(TCPServer(inbound: inbound))
      }
    }

    public nonisolated func withEachClient(
      _ acceptClient: @Sendable @escaping (Client) async throws(CancellationError) -> Void
    ) async throws(ConnectionError) {
      try await withThrowingDiscardingTaskGroup { group in
        for try await client in inbound {
          group.addTask {
            return try await client.executeThenClose { inbound, outbound in
              let socket = TCPSocket(inbound: inbound, outbound: outbound)
              return try await acceptClient(socket)
            }
          }
        }
      }
    }
  }
#endif
