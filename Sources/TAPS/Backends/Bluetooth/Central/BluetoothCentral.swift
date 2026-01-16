import AsyncAlgorithms
internal import Bluetooth
import Logging
public import ServiceLifecycle

#if canImport(FoundationEssentials)
  internal import FoundationEssentials
#else
  internal import Foundation
#endif

public struct BluetoothService: Sendable, Identifiable {
  public let id: BluetoothUUID
  public let isPrimary: Bool
}

public struct BluetoothAdvertisement: Sendable {
  public struct ServiceData: Sendable {
    public let id: BluetoothUUID
    internal let data: Data

    init(id: BluetoothUUID, data: Data) {
      self.id = id
      self.data = data
    }

    public func withServiceData<T, E: Error>(
      _ perform: (borrowing Span<UInt8>) throws(E) -> T
    ) throws(E) -> T {
      try perform(data.span)
    }

    internal init(id: Bluetooth.BluetoothUUID, data: Data) {
      self.id = BluetoothUUID(uuid: id)
      self.data = data
    }
  }

  private let advertisementData: AdvertisementData

  internal init(data: AdvertisementData) {
    self.advertisementData = data
  }

  public var localName: String? { advertisementData.localName }
  public var serviceData: [ServiceData]? {
    advertisementData.serviceData.isEmpty ? nil : advertisementData.serviceData.map { id, data in
      ServiceData(id: BluetoothUUID(uuid: id), data: data)
    }
  }
  public var serviceUUIDs: [BluetoothUUID]? {
    advertisementData.serviceUUIDs.isEmpty ? nil : advertisementData.serviceUUIDs.map(BluetoothUUID.init)
  }
}

public actor BluetoothCentral {
  public struct Peer: Sendable {
    internal let scanResult: ScanResult
    public let name: String?
    internal let _discoveredAt: Date

    public var discoveredAt: ContinuousClock.Instant {
      // Convert Date to ContinuousClock.Instant (approximate)
      ContinuousClock.now
    }

    public var isConnectable: Bool {
      // Check if connectable flag is in advertisement data
      true
    }
    public var rssi: RSSI? {
      RSSI(Int8(clamping: scanResult.rssi))
    }
    public var advertisement: BluetoothAdvertisement {
      BluetoothAdvertisement(data: scanResult.advertisementData)
    }
  }

  public actor Peripheral: Sendable, ServiceLifecycle.Service {
    nonisolated let connection: PeripheralConnection
    nonisolated let central: BluetoothCentral

    internal init(connection: PeripheralConnection, central: BluetoothCentral) {
      self.connection = connection
      self.central = central
    }

    public func run() async throws {
      try await gracefulShutdown()
    }
  }

  internal nonisolated let centralManager: CentralManager
  private let inbound = AsyncChannel<_NetworkBytes>()

  internal init() async throws {
    self.centralManager = CentralManager()

    // Wait for Bluetooth to be powered on
    while true {
      let state = await centralManager.state()
      switch state {
      case .poweredOn:
        return
      case .unknown, .resetting, .unsupported, .unauthorized, .poweredOff:
        try await Task.sleep(for: .seconds(1))
      }
    }
  }

  public func listServices(for peer: Peer) async throws -> [BluetoothService] {
    let connection = try await centralManager.connect(to: peer.scanResult.peripheral)
    let services = try await connection.discoverServices()
    return services.map { service in
      BluetoothService(
        id: BluetoothUUID(uuid: service.uuid),
        isPrimary: service.isPrimary
      )
    }
  }

  internal func withConnection<T: Sendable>(
    _ peripheral: Peripheral,
    perform: (Peripheral) async throws -> T
  ) async throws -> T {
    do {
      let result = try await perform(peripheral)
      await peripheral.connection.disconnect()
      return result
    } catch {
      await peripheral.connection.disconnect()
      throw error
    }
  }

  internal func connect(to scanResult: ScanResult) async throws -> Peripheral {
    let connection = try await centralManager.connect(to: scanResult.peripheral)
    return Peripheral(connection: connection, central: self)
  }
}
