internal import AsyncDNSResolver

public actor MDNSClient: PeerDiscoveryMechanismProtocol {
  public struct Reference: Sendable {
    internal enum Underlying: Sendable {
      case ptr(String)
      case srv(String)
    }

    let underlying: Underlying

    public static func ptr(to host: String) -> Reference {
      Reference(underlying: .ptr(host))
    }

    public static func srv(to host: String) -> Reference {
      Reference(underlying: .ptr(host))
    }
  }
  public struct Peer: Sendable {
    public let hostname: String
    public let port: Int
  }

  fileprivate init() {}

  public static func withMDNS<T: Sendable>(
    perform: (MDNSClient) async throws -> T
  ) async throws -> T {
    let mdns = MDNSClient()
    return try await perform(mdns)
  }

  internal func discoverTXT(toHost name: String) async throws -> [TXTRecord] {
    let resolver = try AsyncDNSResolver()
    return try await resolver.queryTXT(name: name)
  }

  internal nonisolated func discover(_ reference: Reference) async throws -> [Peer] {
    var peers = [Peer]()
    let resolver = try AsyncDNSResolver()

    switch reference.underlying {
    case .ptr(let name):
      let ptr = try await resolver.queryPTR(name: name)
      for name in ptr.names {
        guard let srv = try await resolver.querySRV(name: name).first else {
          continue
        }

        let peer = Peer(
          hostname: srv.host,
          port: Int(srv.port)
        )

        // Prevent duplicates
        if !peers.contains(where: { $0.hostname == peer.hostname }) {
          peers.append(peer)
        }
      }

      return peers
    case .srv(let name):
      guard let srv = try await resolver.querySRV(name: name).first else {
        return []
      }

      let peer = Peer(
        hostname: srv.host,
        port: Int(srv.port)
      )

      return [peer]
    }
  }

  public func withDiscovery(
    of reference: Reference,
    pollingInterval: Duration? = .seconds(5),
    handleResults: @Sendable ([Peer]) async throws -> Void
  ) async throws {
    while !Task.isCancelled {
      let peers = try await discover(reference)
      try await handleResults(peers)

      if let pollingInterval {
        try await Task.sleep(for: pollingInterval)
      } else {
        return
      }
    }
  }
}

extension PeerDiscoveryMechanism where Mechanism == MDNSClient {
  public static var mdns: PeerDiscoveryMechanism<MDNSClient> {
    PeerDiscoveryMechanism { context in
      MDNSClient()
    }
  }
}
