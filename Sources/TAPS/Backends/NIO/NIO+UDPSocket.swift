#if canImport(NIOPosix)
  import AsyncAlgorithms
  internal import NIOCore
  internal import NIOPosix
  import Logging
  import ServiceLifecycle

  /// Async sequence wrapper for UDP inbound datagrams
  @available(macOS 15.0, *)
  public struct UDPInboundDatagramSequence: AsyncSequence, Sendable {
    public typealias Element = UDPInboundDatagram

    internal let stream: NIOAsyncChannelInboundStream<AddressedEnvelope<ByteBuffer>>

    public struct AsyncIterator: AsyncIteratorProtocol {
      internal var iterator: NIOAsyncChannelInboundStream<AddressedEnvelope<ByteBuffer>>.AsyncIterator

      public mutating func next() async throws -> UDPInboundDatagram? {
        guard let envelope = try await iterator.next() else {
          return nil
        }
        return UDPInboundDatagram(
          remoteAddress: SocketAddress(from: envelope.remoteAddress),
          data: NetworkInputBytes(buffer: envelope.data)
        )
      }
    }

    public func makeAsyncIterator() -> AsyncIterator {
      AsyncIterator(iterator: stream.makeAsyncIterator())
    }
  }

  /// UDP Socket implementation using SwiftNIO
  @available(macOS 15.0, *)
  public struct UDPSocket: DatagramSocketProtocol {
    public typealias SocketError = any Error
    public typealias InboundDatagram = UDPInboundDatagram
    public typealias OutboundDatagram = UDPOutboundDatagram
    public typealias InboundDatagrams = UDPInboundDatagramSequence

    private nonisolated let _inbound: NIOAsyncChannelInboundStream<AddressedEnvelope<ByteBuffer>>
    private nonisolated let outbound: NIOAsyncChannelOutboundWriter<AddressedEnvelope<ByteBuffer>>
    private nonisolated let underlyingChannel: any Channel
    private nonisolated let logger = Logger(label: "engineer.edge.taps.udp")

    public nonisolated var inbound: InboundDatagrams {
      UDPInboundDatagramSequence(stream: _inbound)
    }

    internal init(
      inbound: NIOAsyncChannelInboundStream<AddressedEnvelope<ByteBuffer>>,
      outbound: NIOAsyncChannelOutboundWriter<AddressedEnvelope<ByteBuffer>>,
      channel: any Channel
    ) {
      self._inbound = inbound
      self.outbound = outbound
      self.underlyingChannel = channel
    }

    public func run() async throws {
      try await gracefulShutdown()
    }

    public func send(_ datagram: OutboundDatagram) async throws {
      let remoteAddress = try NIOCore.SocketAddress(
        ipAddress: datagram.remoteAddress.host,
        port: datagram.remoteAddress.port
      )
      let envelope = AddressedEnvelope(
        remoteAddress: remoteAddress,
        data: datagram.data.buffer
      )
      try await outbound.write(envelope)
    }

    /// Join a multicast group
    public func joinGroup(_ group: MulticastGroup) async throws {
      let groupAddress = try NIOCore.SocketAddress(
        ipAddress: group.address,
        port: group.port
      )

      guard let multicastChannel = underlyingChannel as? MulticastChannel else {
        throw UDPSocketError.multicastNotSupported
      }

      if let interface = group.interface {
        let device = try NIONetworkDevice.find(named: interface)
        try await multicastChannel.joinGroup(groupAddress, device: device).get()
      } else {
        try await multicastChannel.joinGroup(groupAddress).get()
      }
    }

    /// Leave a multicast group
    public func leaveGroup(_ group: MulticastGroup) async throws {
      let groupAddress = try NIOCore.SocketAddress(
        ipAddress: group.address,
        port: group.port
      )

      guard let multicastChannel = underlyingChannel as? MulticastChannel else {
        throw UDPSocketError.multicastNotSupported
      }

      if let interface = group.interface {
        let device = try NIONetworkDevice.find(named: interface)
        try await multicastChannel.leaveGroup(groupAddress, device: device).get()
      } else {
        try await multicastChannel.leaveGroup(groupAddress).get()
      }
    }

    /// Create a UDP socket and run the operation
    internal static func withSocket<T: Sendable>(
      parameters: UDPServiceParameters,
      context: TAPSContext,
      perform: @escaping @Sendable (UDPSocket) async throws -> T
    ) async throws -> T {
      var bootstrap = DatagramBootstrap(group: .singletonMultiThreadedEventLoopGroup)
        .channelOption(
          ChannelOptions.socketOption(.so_reuseaddr),
          value: SocketOptionValue(parameters.socket.reuseAddress ? 1 : 0)
        )

      #if os(Linux)
        if parameters.socket.reusePort {
          bootstrap = bootstrap.channelOption(
            ChannelOptions.socketOption(.so_reuseport),
            value: SocketOptionValue(1)
          )
        }
      #endif

      if parameters.socket.allowBroadcast {
        bootstrap = bootstrap.channelOption(
          ChannelOptions.socketOption(.so_broadcast),
          value: SocketOptionValue(1)
        )
      }

      let bindHost = parameters.socket.bindHost ?? "0.0.0.0"
      let bindPort = parameters.socket.bindPort

      let channel = try await bootstrap
        .bind(host: bindHost, port: bindPort)
        .flatMapThrowing { channel in
          try NIOAsyncChannel<
            AddressedEnvelope<ByteBuffer>,
            AddressedEnvelope<ByteBuffer>
          >(wrappingChannelSynchronously: channel)
        }
        .get()

      return try await channel.executeThenClose { inbound, outbound in
        let socket = UDPSocket(
          inbound: inbound,
          outbound: outbound,
          channel: channel.channel
        )

        // Configure multicast if specified
        if let multicast = parameters.multicast {
          // Set multicast TTL
          _ = channel.channel.setOption(
            ChannelOptions.ipOption(.ip_multicast_ttl),
            value: CInt(multicast.multicastTTL)
          )

          // Set multicast loopback
          _ = channel.channel.setOption(
            ChannelOptions.ipOption(.ip_multicast_loop),
            value: CInt(multicast.multicastLoopback ? 1 : 0)
          )

          // Join multicast groups
          for group in multicast.joinGroups {
            try await socket.joinGroup(group)
          }
        }

        return try await perform(socket)
      }
    }
  }

  /// Simple socket address wrapper
  public struct SocketAddress: Sendable, Hashable {
    public var host: String
    public var port: Int

    public init(host: String, port: Int) {
      self.host = host
      self.port = port
    }

    internal init(from nioAddress: NIOCore.SocketAddress) {
      self.host = nioAddress.ipAddress ?? "0.0.0.0"
      self.port = nioAddress.port ?? 0
    }
  }

  /// Inbound UDP datagram with source address
  public struct UDPInboundDatagram: Sendable {
    public var remoteAddress: SocketAddress
    public var data: NetworkInputBytes

    public init(remoteAddress: SocketAddress, data: NetworkInputBytes) {
      self.remoteAddress = remoteAddress
      self.data = data
    }
  }

  /// Outbound UDP datagram with destination address
  public struct UDPOutboundDatagram: Sendable {
    public var remoteAddress: SocketAddress
    public var data: NetworkOutputBytes

    public init(to remoteAddress: SocketAddress, data: NetworkOutputBytes) {
      self.remoteAddress = remoteAddress
      self.data = data
    }

    public init(host: String, port: Int, data: NetworkOutputBytes) {
      self.remoteAddress = SocketAddress(host: host, port: port)
      self.data = data
    }
  }

  extension NIONetworkDevice {
    static func find(named name: String) throws -> NIONetworkDevice {
      let devices = try System.enumerateDevices()
      guard let device = devices.first(where: { $0.name == name }) else {
        throw UDPSocketError.interfaceNotFound(name)
      }
      return device
    }
  }

  public enum UDPSocketError: Error {
    case interfaceNotFound(String)
    case invalidAddress(String)
    case multicastNotSupported
  }
#endif
