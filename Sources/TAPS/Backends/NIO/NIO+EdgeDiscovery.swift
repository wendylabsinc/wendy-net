internal import AsyncDNSResolver

public actor EdgeDiscoveryClient: PeerDiscoveryMechanismProtocol {
  public struct Reference: Sendable {
    public static func any() -> Reference {
      Reference()
    }
  }

  public struct Peer: Sendable, Hashable {
    public enum Host: Sendable, Hashable {
      public struct InternetHost: Sendable, Hashable {
        public let hostname: String
        public let port: Int
      }

      case internet(InternetHost)
    }

    public let host: Host
    internal let txt: [TXTRecord]
  }

  fileprivate init() {}

  public static func withEdgeDiscovery<T: Sendable>(
    to nameserver: String,
    perform: (EdgeDiscoveryClient) async throws -> T
  ) async throws -> T {
    let mdns = EdgeDiscoveryClient()
    return try await perform(mdns)
  }

  public nonisolated func withDiscovery(
    of reference: Reference,
    pollingInterval: Duration? = .seconds(5),
    handleResults: @Sendable ([Peer]) async throws -> Void
  ) async throws {
    actor Output {
      var results = [Peer]()

      func append(_ peer: Peer) {
        self.results.append(peer)
      }
    }

    // TODO: Bluetooth in parallel
    let output = Output()
    try await MDNSClient.withMDNS { client in
      try await client.withDiscovery(
        of: .ptr(to: "_edgeos._udp.local"),
        pollingInterval: pollingInterval
      ) { devices in
        nextDevice: for device in devices {
          let host = Peer.Host.internet(
            .init(hostname: device.hostname, port: device.port)
          )
          if await output.results.contains(where: { $0.host == host }) {
            continue nextDevice
          }

          let txt = try await client.discoverTXT(toHost: device.hostname)
          await output.append(Peer(host: host, txt: txt))
        }

        try await handleResults(output.results)
      }
    }
  }
}

extension PeerDiscoveryMechanism where Mechanism == EdgeDiscoveryClient {
  public static var mdns: PeerDiscoveryMechanism<EdgeDiscoveryClient> {
    PeerDiscoveryMechanism { context in
      EdgeDiscoveryClient()
    }
  }
}
