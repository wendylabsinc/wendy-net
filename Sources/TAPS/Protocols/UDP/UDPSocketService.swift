/// Combined parameters for UDP socket service
public struct UDPServiceParameters: ParametersWithDefault, Sendable {
  public var socket: UDPSocketParameters
  public var multicast: UDPMulticastParameters?

  public init(
    socket: UDPSocketParameters = .defaultParameters,
    multicast: UDPMulticastParameters? = nil
  ) {
    self.socket = socket
    self.multicast = multicast
  }

  public static var defaultParameters: UDPServiceParameters {
    return UDPServiceParameters()
  }
}

/// UDP socket service implementing DatagramServiceProtocol
@available(macOS 15.0, *)
public struct UDPSocketService: DatagramServiceProtocol {
  public typealias Parameters = UDPServiceParameters
  public typealias Socket = UDPSocket

  private let configuredParameters: UDPServiceParameters

  internal init(parameters: UDPServiceParameters) {
    self.configuredParameters = parameters
  }

  public func withSocket<T: Sendable>(
    parameters: Parameters,
    context: TAPSContext,
    perform: @escaping @Sendable (Socket) async throws -> T
  ) async throws -> T {
    // Merge configured parameters with runtime parameters
    var mergedParams = configuredParameters
    // Runtime parameters can override configured ones if needed
    if parameters.socket.bindPort != 0 {
      mergedParams.socket.bindPort = parameters.socket.bindPort
    }
    if parameters.socket.bindHost != nil {
      mergedParams.socket.bindHost = parameters.socket.bindHost
    }
    if parameters.multicast != nil {
      mergedParams.multicast = parameters.multicast
    }

    return try await UDPSocket.withSocket(
      parameters: mergedParams,
      context: context,
      perform: perform
    )
  }
}

// MARK: - Static Factory Methods

@available(macOS 15.0, *)
extension DatagramServiceProtocol where Self == UDPSocketService {
  /// Create a basic UDP socket
  /// - Parameters:
  ///   - bindHost: Interface to bind to (nil = any)
  ///   - port: Port to bind to (0 = ephemeral)
  /// - Returns: A configured UDP socket service
  public static func udp(
    bindHost: String? = nil,
    port: Int = 0
  ) -> UDPSocketService {
    UDPSocketService(
      parameters: UDPServiceParameters(
        socket: UDPSocketParameters(
          bindHost: bindHost,
          bindPort: port
        )
      )
    )
  }

  /// Create a multicast UDP socket
  /// - Parameters:
  ///   - port: Port to bind to (should match multicast group port)
  ///   - groups: Multicast groups to automatically join
  ///   - multicastTTL: Hop limit for outgoing multicast packets
  ///   - multicastLoopback: Whether to receive own multicast messages
  /// - Returns: A configured multicast UDP socket service
  public static func multicastUDP(
    port: Int,
    groups: [MulticastGroup],
    multicastTTL: UInt8 = 1,
    multicastLoopback: Bool = true
  ) -> UDPSocketService {
    UDPSocketService(
      parameters: UDPServiceParameters(
        socket: UDPSocketParameters(
          bindPort: port,
          reuseAddress: true,
          reusePort: true
        ),
        multicast: UDPMulticastParameters(
          joinGroups: groups,
          multicastTTL: multicastTTL,
          multicastLoopback: multicastLoopback
        )
      )
    )
  }

  /// Create a broadcast-enabled UDP socket
  /// - Parameter port: Port to bind to (0 = ephemeral)
  /// - Returns: A configured broadcast UDP socket service
  public static func broadcastUDP(
    port: Int = 0
  ) -> UDPSocketService {
    UDPSocketService(
      parameters: UDPServiceParameters(
        socket: UDPSocketParameters(
          bindPort: port,
          reuseAddress: true,
          allowBroadcast: true
        )
      )
    )
  }
}
