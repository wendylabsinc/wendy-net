import AsyncAlgorithms
internal import Bluetooth
internal import GATT

#if canImport(DarwinGATT)
  internal import DarwinGATT
#elseif canImport(BluetoothLinux)
  internal import BluetoothLinux
#endif

#if canImport(FoundationEssentials)
  import FoundationEssentials
#else
  import Foundation
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
            $0.data.peripheral == peer.data.peripheral
          }
          self.peers.append(peer)
        }
      }

      let stream = try await central.central.scan(filterDuplicates: true)
      let output = Output()

      try await withTaskCancellationHandler {
        for try await scanData in stream {
          var name: String? = nil
          #if canImport(DarwinGATT)
            name = try? await central.central.name(for: scanData.peripheral)
          #endif
          let peer = Peer(data: scanData, name: name)

          switch reference.underlying {
          case .any, .named(name):
            await output.upsert(peer)
          case .named:
            ()
          }

          try await handleResults(output.peers)

          if pollingInterval == nil {
            stream.stop()
          }
        }
      } onCancel: {
        stream.stop()
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
