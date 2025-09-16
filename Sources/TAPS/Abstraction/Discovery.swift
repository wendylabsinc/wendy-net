public protocol PeerDiscoveryMechanismProtocol<Reference, Peer>: Sendable {
  associatedtype Reference: Sendable
  associatedtype Peer: Sendable

  func withDiscovery(
    of reference: Reference,
    pollingInterval: Duration?,
    handleResults: @Sendable ([Peer]) async throws -> Void
  ) async throws
}

fileprivate actor Output<Peer: Sendable> {
  var peers = [Peer]()
  func update(to peers: [Peer]) {
    self.peers = peers
  }
}

extension PeerDiscoveryMechanismProtocol {
  public func discover(_ reference: Reference) async throws -> [Peer] {
    let output = Output<Peer>()
    do {
      try await withDiscovery(
        of: reference,
        pollingInterval: .seconds(1)
      ) { results in
        await output.update(to: results)
        if !results.isEmpty {
          // TODO: Remove this workaround
          throw CancellationError()
        }
      }
    } catch is CancellationError {
      // Cancellation is fine
    }
    return await output.peers
  }
}

public protocol InternetHost: Sendable {
  var hostname: String { get }
  var port: Int { get }
}

internal struct PeerDiscoveryError: Error {
  static func cannotResolve() -> PeerDiscoveryError {
    PeerDiscoveryError()
  }
}

public struct PeerDiscoveryMechanism<Mechanism: PeerDiscoveryMechanismProtocol>: Sendable {
  internal typealias MakeMechanism = @Sendable (TAPSContext) async throws -> Mechanism
  let makeMechanism: MakeMechanism

  internal init(makeMechanism: @escaping MakeMechanism) {
    self.makeMechanism = makeMechanism
  }

  public func withMechanism<T>(
    forContext context: TAPSContext,
    perform: (inout Mechanism) async throws -> T
  ) async throws -> T {
    var mechanism = try await makeMechanism(context)
    return try await perform(&mechanism)
  }
}
