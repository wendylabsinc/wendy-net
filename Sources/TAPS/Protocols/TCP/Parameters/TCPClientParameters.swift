public struct TCPClientParameters: ParametersWithDefault {
  public var connectionTimeout: Duration
  public var socketParameters: TCPSocketParameters

  public init(
    connectionTimeout: Duration = .seconds(30),
    socketParameters: TCPSocketParameters = .defaultParameters
  ) {
    self.connectionTimeout = connectionTimeout
    self.socketParameters = socketParameters
  }

  public static var defaultParameters: TCPClientParameters {
    return TCPClientParameters()
  }
}

#if canImport(NIOPosix)
  internal import NIOPosix
  internal import NIOCore

  extension ClientBootstrap {
    func applyParameters(_ parameters: TCPClientParameters) -> ClientBootstrap {
      self.connectTimeout(TimeAmount(parameters.connectionTimeout))
    }
  }
#endif
