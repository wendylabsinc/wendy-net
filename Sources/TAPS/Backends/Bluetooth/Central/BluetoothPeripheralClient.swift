import AsyncAlgorithms
internal import Bluetooth
import Logging
internal import NIOCore
internal import NIOPosix

#if canImport(FoundationEssentials)
  internal import FoundationEssentials
#else
  internal import Foundation
#endif

extension BluetoothCentral.Peripheral {
  public struct Service: Sendable, Identifiable {
    fileprivate let underlying: GATTService
    public var id: BluetoothUUID { BluetoothUUID(uuid: underlying.uuid) }
    public var isPrimary: Bool { underlying.isPrimary }
  }

  public struct Characteristic: Sendable, Identifiable {
    fileprivate let underlying: GATTCharacteristic
    public var id: BluetoothUUID { BluetoothUUID(uuid: underlying.uuid) }
  }

  public var isConnected: Bool {
    get async {
      let state = await connection.state()
      return state == .connected
    }
  }

  public var rssi: RSSI {
    get async throws {
      let rssiValue = try await connection.readRSSI()
      return RSSI(unchecked: Int8(clamping: rssiValue))
    }
  }

  public var services: [Service] {
    get async throws {
      let discoveredServices = try await connection.discoverServices()
      return discoveredServices.map { service in
        return Service(underlying: service)
      }
    }
  }

  public func listCharacteristics(for service: Service) async throws -> [Characteristic] {
    let characteristics = try await connection.discoverCharacteristics(for: service.underlying)
    return characteristics.map(Characteristic.init)
  }

  public func getCharacteristic(
    _ characteristicId: Characteristic.ID,
    forService serviceId: BluetoothService.ID
  ) async throws -> Characteristic? {
    for service in try await services where service.id == serviceId {
      let characteristics = try await listCharacteristics(for: service)
      for characteristic in characteristics where characteristic.id == characteristicId {
        return characteristic
      }
    }

    return nil
  }

  public func readNotifications(
    forCharacteristic characteristic: Characteristic,
    perform: ([UInt8]) async throws -> Void
  ) async throws {
    let notifications = try await connection.notifications(for: characteristic.underlying)
    for try await notification in notifications {
      let bytes: [UInt8]
      switch notification {
      case .notification(let value):
        bytes = [UInt8](value)
      case .indication(let value):
        bytes = [UInt8](value)
      }
      try await perform(bytes)
    }
  }

  public func writeNotification(
    forCharacteristic characteristic: Characteristic,
    _ span: borrowing Span<UInt8>
  ) async throws {
    var bytes = [UInt8]()
    bytes.reserveCapacity(span.count)
    for i in 0..<span.count {
      bytes.append(span[i])
    }
    let data = Data(bytes)

    try await connection.writeValue(
      data,
      for: characteristic.underlying,
      type: .withResponse
    )
  }

  internal func observeCharacteristic(
    _ characteristic: Characteristic,
    perform: (Data) async throws -> Void
  ) async throws {
    while !Task.isCancelled {
      let value = try await connection.readValue(for: characteristic.underlying)
      try await perform(value)
    }
  }

  public func observeCharacteristic<Value: Sendable>(
    _ characteristic: BluetoothCharacteristic<Value>,
    perform: (Value) async throws -> Void
  ) async throws {
    let discoveredServices = try await connection.discoverServices([characteristic.serviceId.uuid])

    guard let service = discoveredServices.first else {
      return
    }

    let characteristics = try await connection.discoverCharacteristics(
      for: service
    )

    guard let gattCharacteristic = characteristics.first(where: { $0.uuid == characteristic.id.uuid }) else {
      return
    }

    try await observeCharacteristic(
      Characteristic(underlying: gattCharacteristic)
    ) { value in
      let parsedValue = try characteristic.parse(value.span)
      try await perform(parsedValue)
    }
  }
}
