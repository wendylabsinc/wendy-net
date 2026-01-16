import AsyncAlgorithms
internal import Bluetooth

#if canImport(FoundationEssentials)
  internal import FoundationEssentials
#else
  internal import Foundation
#endif

extension BluetoothCentral {
  public struct PeripheralDiscovery: PeerDiscoveryMechanismProtocol {
    public struct Reference: Sendable {
      internal enum Underlying: Sendable {
        case any
        case named(String)
      }

      let underlying: Underlying

      public static var any: Reference {
        Reference(underlying: .any)
      }

      public static func named(_ name: String) -> Reference {
        Reference(underlying: .named(name))
      }
    }

    let central: BluetoothCentral

    public nonisolated func withDiscovery(
      of reference: Reference,
      pollingInterval: Duration? = .seconds(5),
      handleResults: @Sendable ([Peer]) async throws -> Void
    ) async throws {
      actor Output {
        var peers = [Peer]()

        func upsert(_ peer: Peer) {
          self.peers.removeAll {
            $0.scanResult.peripheral == peer.scanResult.peripheral
          }
          self.peers.append(peer)
        }
      }

      let stream = try await central.centralManager.scan()
      let output = Output()

      try await withTaskCancellationHandler {
        for try await scanResult in stream {
          let name = scanResult.advertisementData.localName
          let peer = Peer(scanResult: scanResult, name: name, _discoveredAt: scanResult.timestamp)

          switch reference.underlying {
          case .any, .named(name):
            await output.upsert(peer)
          case .named:
            ()
          }

          try await handleResults(output.peers)

          if pollingInterval == nil {
            try await central.centralManager.stopScan()
            break
          }
        }
      } onCancel: {
        Task {
          try? await central.centralManager.stopScan()
        }
      }
    }
  }
}

extension PeerDiscoveryMechanism where Mechanism == BluetoothCentral.PeripheralDiscovery {
  public static var nearbyPeripherals: PeerDiscoveryMechanism<BluetoothCentral.PeripheralDiscovery>
  {
    PeerDiscoveryMechanism { context in
      BluetoothCentral.PeripheralDiscovery(
        central: context.bluetoothCentral
      )
    }
  }
}
