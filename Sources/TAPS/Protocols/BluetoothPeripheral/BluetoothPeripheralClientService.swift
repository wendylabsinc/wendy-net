#if canImport(FoundationEssentials)
  import FoundationEssentials
#else
  import Foundation
#endif

public struct BluetoothPeripheralClientService: ClientServiceProtocol {
  public typealias Parameters = BluetoothPeripheralClientParameters
  public typealias Client = BluetoothCentral.Peripheral

  let resolve: @Sendable (TAPSContext) async throws -> BluetoothCentral.Peer

  internal init(
    resolve: @escaping @Sendable (TAPSContext) async throws -> BluetoothCentral.Peer
  ) {
    self.resolve = resolve
  }

  /// Create Bluetooth peripheral client with given parameters
  public func withConnection<T: Sendable>(
    parameters: Parameters,
    context: TAPSContext,
    perform: @escaping @Sendable (Client) async throws -> T
  ) async throws -> T {
    let peer = try await resolve(context)
    let peripheral = try await context.bluetoothCentral.connect(to: peer.scanResult)

    return try await context.bluetoothCentral.withConnection(peripheral) { peripheral in
      try await perform(peripheral)
    }
  }
}

extension ClientServiceProtocol where Self == BluetoothPeripheralClientService {
  public static func bluetoothPeripheral(
    _ reference: BluetoothCentral.PeripheralDiscovery.Reference
  ) -> BluetoothPeripheralClientService {
    BluetoothPeripheralClientService { context in
      let resolver = BluetoothCentral.PeripheralDiscovery(central: context.bluetoothCentral)
      let peers = try await resolver.discover(reference)
      guard let peer = peers.first else {
        throw PeerDiscoveryError.cannotResolve()
      }

      return peer
    }
  }
}
