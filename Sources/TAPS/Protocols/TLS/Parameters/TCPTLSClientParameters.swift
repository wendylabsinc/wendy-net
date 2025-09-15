extension TLSClientParameters {
  public struct TCP: ParametersWithDefault {
    public var tcp: TCPClientParameters
    public var tls: TLSClientParameters

    public init(
      tcp: TCPClientParameters = .defaultParameters,
      tls: TLSClientParameters = .defaultParameters
    ) {
      self.tcp = tcp
      self.tls = tls
    }

    public static var defaultParameters: TLSClientParameters.TCP {
      TLSClientParameters.TCP(
        tcp: .defaultParameters,
        tls: .defaultParameters
      )
    }
  }

}
