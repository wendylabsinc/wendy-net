/// Parameters for UDP socket configuration
public struct UDPSocketParameters: ParametersWithDefault, Sendable {
  /// Interface to bind to (nil = any interface)
  public var bindHost: String?

  /// Port to bind to (0 = ephemeral port)
  public var bindPort: Int

  /// Enable SO_REUSEADDR socket option
  public var reuseAddress: Bool

  /// Enable SO_REUSEPORT socket option (needed for multicast)
  public var reusePort: Bool

  /// Enable SO_BROADCAST socket option
  public var allowBroadcast: Bool

  public init(
    bindHost: String? = nil,
    bindPort: Int = 0,
    reuseAddress: Bool = true,
    reusePort: Bool = false,
    allowBroadcast: Bool = false
  ) {
    self.bindHost = bindHost
    self.bindPort = bindPort
    self.reuseAddress = reuseAddress
    self.reusePort = reusePort
    self.allowBroadcast = allowBroadcast
  }

  public static var defaultParameters: UDPSocketParameters {
    return UDPSocketParameters()
  }
}
