/// HTTP service implementing ClientServiceProtocol
public struct HTTP1ClientService: ClientServiceProtocol {
  public typealias Parameters = HTTP1ClientParameters
  public typealias Client = HTTP1Client

  private let host: String
  private let port: Int
  private var tls: TLSClientParameters.TCP?

  public init(host: String, port: Int, tls: TLSClientParameters.TCP?) {
    self.host = host
    self.port = port
    self.tls = tls
  }

  /// Create TCP client with given parameters
  public func withConnection<T: Sendable>(
    parameters: Parameters,
    context: TAPSContext,
    perform: @escaping @Sendable (Client) async throws -> T
  ) async throws -> T {
    return try await HTTP1Client.withConnection(
      host: host,
      port: port,
      tls: tls,
      parameters: parameters,
      context: context,
      perform: perform
    )
  }
}

extension ClientServiceProtocol where Self == HTTP1ClientService {
  public static func http1(host: String, port: Int = 80) -> HTTP1ClientService {
    HTTP1ClientService(
      host: host,
      port: port,
      tls: nil
    )
  }

  public static func https1(host: String, port: Int = 443) -> HTTP1ClientService {
    HTTP1ClientService(
      host: host,
      port: port,
      tls: .defaultParameters
    )
  }
}
