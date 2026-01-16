import AsyncAlgorithms
internal import Bluetooth
import Logging
import ServiceLifecycle

#if canImport(FoundationEssentials)
  internal import FoundationEssentials
#else
  internal import Foundation
#endif

/// ATT attribute permission bitfield values
@frozen
public struct ATTAttributePermissions: OptionSet, Equatable, Hashable, Sendable {
  public let rawValue: UInt8

  public init(rawValue: UInt8) {
    self.rawValue = rawValue
  }

  // Access
  public static var read: ATTAttributePermissions { ATTAttributePermissions(rawValue: 0x01) }
  public static var write: ATTAttributePermissions { ATTAttributePermissions(rawValue: 0x02) }

  // Encryption
  public static var encrypt: ATTAttributePermissions { [.readEncrypt, .writeEncrypt] }
  public static var readEncrypt: ATTAttributePermissions { ATTAttributePermissions(rawValue: 0x04) }
  public static var writeEncrypt: ATTAttributePermissions { ATTAttributePermissions(rawValue: 0x08) }

  // Authentication
  public static var authentication: ATTAttributePermissions { [.readAuthentication, .writeAuthentication] }
  public static var readAuthentication: ATTAttributePermissions { ATTAttributePermissions(rawValue: 0x10) }
  public static var writeAuthentication: ATTAttributePermissions { ATTAttributePermissions(rawValue: 0x20) }

  // Authorization
  public static var authorized: ATTAttributePermissions { ATTAttributePermissions(rawValue: 0x40) }
  public static var noAuthorization: ATTAttributePermissions { ATTAttributePermissions(rawValue: 0x80) }

  internal var toGATT: GATTAttributePermissions {
    var result = GATTAttributePermissions()
    if contains(.read) { result.insert(.readable) }
    if contains(.write) { result.insert(.writeable) }
    if contains(.readEncrypt) { result.insert(.readEncryptionRequired) }
    if contains(.writeEncrypt) { result.insert(.writeEncryptionRequired) }
    return result
  }
}

/// GATT Characteristic Properties Bitfield values
public struct GATTCharacteristicProperty: OptionSet, Hashable, Sendable {
  public var rawValue: UInt8

  public init(rawValue: UInt8) {
    self.rawValue = rawValue
  }

  public static var broadcast: GATTCharacteristicProperty { GATTCharacteristicProperty(rawValue: 0x01) }
  public static var read: GATTCharacteristicProperty { GATTCharacteristicProperty(rawValue: 0x02) }
  public static var writeWithoutResponse: GATTCharacteristicProperty { GATTCharacteristicProperty(rawValue: 0x04) }
  public static var write: GATTCharacteristicProperty { GATTCharacteristicProperty(rawValue: 0x08) }
  public static var notify: GATTCharacteristicProperty { GATTCharacteristicProperty(rawValue: 0x10) }
  public static var indicate: GATTCharacteristicProperty { GATTCharacteristicProperty(rawValue: 0x20) }
  public static var signedWrite: GATTCharacteristicProperty { GATTCharacteristicProperty(rawValue: 0x40) }
  public static var extendedProperties: GATTCharacteristicProperty { GATTCharacteristicProperty(rawValue: 0x80) }

  internal var toGATT: GATTCharacteristicProperties {
    var result = GATTCharacteristicProperties()
    if contains(.broadcast) { result.insert(.broadcast) }
    if contains(.read) { result.insert(.read) }
    if contains(.writeWithoutResponse) { result.insert(.writeWithoutResponse) }
    if contains(.write) { result.insert(.write) }
    if contains(.notify) { result.insert(.notify) }
    if contains(.indicate) { result.insert(.indicate) }
    if contains(.signedWrite) { result.insert(.authenticatedSignedWrites) }
    if contains(.extendedProperties) { result.insert(.extendedProperties) }
    return result
  }
}

public struct BluetoothCharacteristicsWriter: ~Copyable {
  public typealias ProduceEvents<Value: Sendable> = @Sendable (Value) async throws -> Void

  internal let peripheral: BluetoothPeripheral
  internal var taskGroup: ThrowingDiscardingTaskGroup<any Error>
  let registerCharacteristics:
    @Sendable (BluetoothUUID, borrowing BluetoothCharacteristicsWriter) async throws -> (
      GATTServiceRegistration, [GATTCharacteristic]
    )
  internal var characteristics = [GATTCharacteristicDefinition]()

  public struct RegisteringCharacteristic<Value: Sendable>: Sendable {
    public var characteristic: BluetoothCharacteristic<Value>
    public var initialValue: Value
    public var withValues: @Sendable (ProduceEvents<Value>) async throws -> Void
    public var permissions: ATTAttributePermissions
    public var properties: GATTCharacteristicProperty
  }

  public mutating func add<each Value: Sendable>(
    serviceId: BluetoothUUID,
    characteristics: repeat RegisteringCharacteristic<each Value>
  ) async throws {
    for registration in repeat each characteristics {
      precondition(
        serviceId == registration.characteristic.serviceId,
        "ServiceID did not match that specified in the characteristic")

      let write = registration.characteristic.write(registration.initialValue)
      write { bytes in
        let underlying = GATTCharacteristicDefinition(
          uuid: registration.characteristic.id.uuid,
          properties: registration.properties.toGATT,
          permissions: registration.permissions.toGATT,
          initialValue: Data(bytes)
        )
        self.characteristics.append(underlying)
      }

      let (_, registeredCharacteristics) = try await registerCharacteristics(
        registration.characteristic.serviceId, self)

      taskGroup.addTask { [peripheral] in
        guard registeredCharacteristics.count == 1 else {
          preconditionFailure(
            "registeredCharacteristics.count should have been 1, as 1 characteristic was registered")
        }

        let registeredCharacteristic = registeredCharacteristics[0]

        try await registration.withValues { event in
          var data = Data()
          let write = registration.characteristic.write(event)

          write { bytes in
            data = Data(bytes)
          }

          try await peripheral.updateValue(
            data,
            for: registeredCharacteristic
          )
        }
      }
    }
  }

  public mutating func add<Value: Sendable>(
    characteristic: BluetoothCharacteristic<Value>,
    initialValue: Value,
    withValues: @Sendable @escaping (ProduceEvents<Value>) async throws -> Void,
    permissions: ATTAttributePermissions,
    properties: GATTCharacteristicProperty
  ) async throws {
    try await add(
      serviceId: characteristic.serviceId,
      characteristics: RegisteringCharacteristic(
        characteristic: characteristic,
        initialValue: initialValue,
        withValues: withValues,
        permissions: permissions,
        properties: properties
      )
    )
  }
}

public protocol BluetoothServiceProtocol: Sendable {
  var id: BluetoothUUID { get }

  func writeCharacteristics(
    into writer: inout BluetoothCharacteristicsWriter
  ) async throws
}

public actor BluetoothPeripheral {
  fileprivate actor RegisteringServices {
    var ids = [Bluetooth.BluetoothUUID]()

    func append(_ id: Bluetooth.BluetoothUUID) {
      ids.append(id)
    }
  }

  public struct ServiceRegistration: @unchecked Sendable, ~Copyable {
    let peripheral: BluetoothPeripheral
    fileprivate let services: RegisteringServices
    let taskGroup: ThrowingDiscardingTaskGroup<any Error>
    let registerCharacteristics:
      @Sendable (BluetoothUUID, borrowing BluetoothCharacteristicsWriter) async throws -> (
        GATTServiceRegistration, [GATTCharacteristic]
      )

    mutating func register(_ service: some BluetoothServiceProtocol) async throws {
      await services.append(service.id.uuid)

      var writer = BluetoothCharacteristicsWriter(
        peripheral: peripheral,
        taskGroup: taskGroup,
        registerCharacteristics: registerCharacteristics
      )
      try await service.writeCharacteristics(into: &writer)
    }
  }

  private let manager: PeripheralManager
  private nonisolated let inboundWrapper = InboundWrapper()

  private final class InboundWrapper: @unchecked Sendable {
    var inbound: AsyncChannel<Data>?

    init() {}
  }

  internal init() async throws {
    self.manager = PeripheralManager()

    // Wait for Bluetooth to be powered on
    while true {
      let state = await manager.state()
      switch state {
      case .poweredOn:
        return
      case .unknown, .resetting, .unsupported, .unauthorized, .poweredOff:
        try await Task.sleep(for: .seconds(1))
      }
    }
  }

  internal nonisolated func read(
    characteristicId: BluetoothUUID,
    into inbound: AsyncChannel<Data>
  ) {
    // GATT read handling is done through gattRequests() stream
    self.inboundWrapper.inbound = inbound
  }

  internal func updateValue(
    _ value: Data,
    for characteristic: GATTCharacteristic
  ) async throws {
    try await manager.updateValue(value, for: characteristic, type: .notification)
  }

  internal func run(
    localName: String?
  ) async throws {
    try await withTaskCancellationHandler {
      let advertisementData = AdvertisementData(localName: localName)
      let parameters = AdvertisingParameters(isConnectable: true)
      try await manager.startAdvertising(advertisementData, parameters: parameters)

      while true {
        try await Task.sleep(for: .seconds(100_000))
      }
    } onCancel: {
      Task {
        await manager.stopAdvertising()
      }
    }
  }

  internal func run(
    localName: String?,
    registerServices: (inout sending ServiceRegistration) async throws -> Void
  ) async throws {
    try await withTaskCancellationHandler {
      try await withThrowingDiscardingTaskGroup { taskGroup in
        let services = RegisteringServices()

        var registration = ServiceRegistration(
          peripheral: self,
          services: services,
          taskGroup: taskGroup
        ) { [manager] id, writer in
          let serviceDefinition = GATTServiceDefinition(
            uuid: id.uuid,
            isPrimary: true,
            characteristics: writer.characteristics
          )

          let service = try await manager.addService(serviceDefinition)

          // Get the characteristics from the registered service
          let characteristics = service.characteristics

          return (service, characteristics)
        }
        try await registerServices(&registration)

        let advertisementData = AdvertisementData(
          localName: localName,
          serviceUUIDs: await services.ids
        )
        let parameters = AdvertisingParameters(isConnectable: true)
        try await manager.startAdvertising(advertisementData, parameters: parameters)

        // Handle GATT requests
        taskGroup.addTask { [manager, inboundWrapper] in
          do {
            for try await request in try await manager.gattRequests() {
              switch request {
              case .read(let readRequest):
                // Return the current value or empty data
                await readRequest.respond(.success(Data()))
              case .write(let writeRequest):
                // Forward to inbound channel if set
                if let inbound = inboundWrapper.inbound {
                  await inbound.send(writeRequest.value)
                }
                if writeRequest.writeType == .withResponse {
                  await writeRequest.respond(.success(()))
                }
              case .readDescriptor(let descriptorRequest):
                await descriptorRequest.respond(.success(Data()))
              case .writeDescriptor(let descriptorRequest):
                await descriptorRequest.respond(.success(()))
              case .executeWrite(let executeRequest):
                await executeRequest.respond(.success(()))
              case .authorize, .subscribe, .unsubscribe:
                // Handle authorization and subscription changes
                break
              }
            }
          } catch {
            // Ignore GATT request stream errors
          }
        }

        try await gracefulShutdown()
        await manager.stopAdvertising()
      }
    } onCancel: {
      Task {
        await manager.stopAdvertising()
      }
    }
  }
}
