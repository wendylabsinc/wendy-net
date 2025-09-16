public struct TCPServerParameters: ParametersWithDefault {
  public var backlog: Int32
  public var socketParameters: TCPSocketParameters

  public init(
    backlog: Int32 = 256,
    socketParameters: TCPSocketParameters = .defaultParameters
  ) {
    self.backlog = backlog
    self.socketParameters = socketParameters
  }

  public static var defaultParameters: TCPServerParameters {
    return TCPServerParameters()
  }
}
