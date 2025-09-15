public struct TCPSocketParameters: ParametersWithDefault {
  public var keepAlive: Bool
  public var noDelay: Bool

  public init(keepAlive: Bool = false, noDelay: Bool = true) {
    self.keepAlive = keepAlive
    self.noDelay = noDelay
  }

  public static var defaultParameters: TCPSocketParameters {
    return TCPSocketParameters()
  }
}
