#if canImport(NIOPosix)
  public import AsyncAlgorithms
  public import HTTPTypes
  internal import NIOCore
  internal import NIOHTTP1
  internal import NIOPosix
  internal import NIOExtras
  internal import NIOHTTPTypes
  internal import NIOHTTPTypesHTTP1
  import Logging

  extension ConnectionSubprotocol<
    ByteBuffer,
    HTTPResponsePart,
    HTTPRequestPart,
    IOData
  > {
    static func http1(
      encoderConfiguration: HTTPRequestEncoder.Configuration = .init(),
      leftOverBytesStrategy: RemoveAfterUpgradeStrategy = .fireError
    ) -> Self {
      return Self {
        HTTPRequestEncoder(configuration: encoderConfiguration)
        ByteToMessageHandler(HTTPResponseDecoder(leftOverBytesStrategy: leftOverBytesStrategy))
        NIOHTTPRequestHeadersValidator()
        HTTP1ToHTTPClientCodec()
      }
    }
  }

  /// TCP Client as proper with real SwiftNIO implementation
  @available(macOS 15.0, *)
  public actor HTTP1Client: DuplexClientProtocol {
    public typealias InboundMessage = HTTPResponse
    public typealias OutboundMessage = HTTPRequest
    public struct ConnectionError: Error {}

    public struct Response: Sendable {
      public let head: HTTPResponse
      public let body: AsyncThrowingChannel<NetworkInputBytes, ConnectionError>
    }

    // Actor-isolated state
    private nonisolated let inbound: NIOAsyncChannelInboundStream<HTTPResponsePart>
    private nonisolated let outbound: NIOAsyncChannelOutboundWriter<HTTPRequestPart>
    private nonisolated let logger = Logger(label: "engineer.edge.taps.http1")
    private var responseHandlers = [
      @Sendable (Result<Response, any Error>) async throws(ConnectionError) -> Void
    ]()

    /// Initialize TCP client with endpoint and parameters
    internal init(
      inbound: NIOAsyncChannelInboundStream<HTTPResponsePart>,
      outbound: NIOAsyncChannelOutboundWriter<HTTPRequestPart>
    ) {
      self.inbound = inbound
      self.outbound = outbound
    }

    internal init(
      socket: TCPSocket<HTTPResponsePart, HTTPRequestPart>
    ) {
      self.inbound = socket._inbound
      self.outbound = socket.outbound
    }

    public func run() async throws {
      do {
        try await withThrowingDiscardingTaskGroup { group in
          var iterator = inbound.makeAsyncIterator()

          nextResponse: while !Task.isCancelled {
            guard
              case .head(let head) = try await iterator.next(),
              !responseHandlers.isEmpty
            else {
              // Protocol error
              throw ConnectionError()
            }

            let handler = responseHandlers.removeFirst()
            let body = AsyncThrowingChannel<NetworkInputBytes, ConnectionError>()

            group.addTask {
              do {
                let response = Response(head: head, body: body)
                try await handler(.success(response))
              } catch is ConnectionError {
                // TODO: body.fail(error)
                body.finish()
              }
            }

            defer { body.finish() }
            while let next = try await iterator.next() {
              switch next {
              case .head:
                // Protocol error
                throw ConnectionError()
              case .body(let buffer):
                await body.send(NetworkInputBytes(buffer: buffer))
              case .end:
                return
              }
            }

            throw CancellationError()
          }
        }

        for handler in responseHandlers {
          try? await handler(.failure(CancellationError()))
        }
      } catch {
        for handler in responseHandlers {
          try? await handler(.failure(error))
        }
      }
    }

    package static func withConnection<T: Sendable>(
      host: String,
      port: Int,
      tls: TLSClientParameters.TCP?,
      parameters: HTTP1ClientParameters,
      context: TAPSContext,
      perform: @escaping @Sendable (HTTP1Client) async throws -> T
    ) async throws -> T {
      let tls = try tls.map { _ in
        try ConnectionSubprotocol.tls(
          configuration: .clientDefault,
          serverHostname: host
        )
      }

      return try await TCPSocket<
        HTTPResponsePart,
        HTTPRequestPart
      >.withClientConnection(
        host: host,
        port: port,
        parameters: parameters.tcp,
        context: context,
        protocolStack: ProtocolStack {
          IODataDuplexHandler()
          if let tls {
            tls
          }
          IODataOutboundDecoder()
          ConnectionSubprotocol.http1()
        }
      ) { socket in
        return try await perform(HTTP1Client(socket: socket))
      }
    }

    private nonisolated func writeRequest<BodyError: Error>(
      _ request: HTTPRequest,
      body: some AsyncSequence<NetworkOutputBytes, BodyError> & Sendable
    ) async throws {
      try await self.outbound.write(.head(request))

      for try await part in body {
        try await self.outbound.write(.body(part.buffer))
      }

      try await self.outbound.write(.end(nil))
    }

    /// Convenience method for sending string messages
    public func withResponse<T: Sendable, E: Error>(
      to request: HTTPRequest,
      body: some AsyncSequence<NetworkOutputBytes, any Error> & Sendable = EmptySequence<
        NetworkOutputBytes, any Error
      >(),
      perform: @Sendable @escaping (Response) async throws(E) -> T
    ) async throws -> T {
      try await withThrowingTaskGroup { group in
        group.addTask {
          try await self.writeRequest(request, body: body)
        }

        return try await withCheckedThrowingContinuation { continuation in
          self.responseHandlers.append { response throws(ConnectionError) -> Void in
            do {
              let result = try await perform(response.get())
              continuation.resume(returning: result)
            } catch {
              continuation.resume(throwing: error)
              throw ConnectionError()
            }
          }
        }
      }
    }
  }

  public struct EmptySequence<Element: Sendable, Error: Swift.Error>: AsyncSequence, Sendable {
    public struct AsyncIterator: AsyncIteratorProtocol {
      public func next() async throws(Error) -> Element? {
        return nil
      }
    }

    public init() {}

    public func makeAsyncIterator() -> AsyncIterator {
      AsyncIterator()
    }
  }
#endif
