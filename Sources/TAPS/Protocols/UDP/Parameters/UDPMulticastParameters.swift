/// Represents a multicast group to join
public struct MulticastGroup: Sendable, Hashable {
  /// The multicast group address (e.g., "239.255.255.250")
  public var address: String

  /// The port number for the multicast group
  public var port: Int

  /// Optional interface to use for multicast (nil = default interface)
  public var interface: String?

  public init(address: String, port: Int, interface: String? = nil) {
    self.address = address
    self.port = port
    self.interface = interface
  }
}

/// Parameters for UDP multicast configuration
public struct UDPMulticastParameters: Sendable {
  /// Multicast groups to automatically join when socket is created
  public var joinGroups: [MulticastGroup]

  /// Multicast TTL / hop limit (default: 1 = local network only)
  public var multicastTTL: UInt8

  /// Whether to receive multicast messages sent by this socket
  public var multicastLoopback: Bool

  public init(
    joinGroups: [MulticastGroup] = [],
    multicastTTL: UInt8 = 1,
    multicastLoopback: Bool = true
  ) {
    self.joinGroups = joinGroups
    self.multicastTTL = multicastTTL
    self.multicastLoopback = multicastLoopback
  }
}
