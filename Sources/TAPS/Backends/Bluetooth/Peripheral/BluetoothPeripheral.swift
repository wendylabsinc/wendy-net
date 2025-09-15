internal import Bluetooth
import Logging
import AsyncAlgorithms
internal import GATT
import ServiceLifecycle

#if canImport(DarwinGATT)
    internal import DarwinGATT
    internal typealias _Peripheral = DarwinPeripheral
#elseif canImport(BluetoothLinux)
    internal import BluetoothLinux
    internal typealias _Peripheral = GATTPeripheral<
        BluetoothLinux.HostController, BluetoothLinux.L2CAPSocket.Server
    >
#endif

#if canImport(FoundationEssentials)
    import FoundationEssentials
#else
    import Foundation
#endif
    
public struct BluetoothCharacteristicsWriter: ~Copyable {
    internal typealias Underlying = GATTAttribute<Data>.Characteristic
    public typealias ProduceEvents<Value: Sendable> = @Sendable (Value) async throws -> Void
    
    internal let peripheral: BluetoothPeripheral
    internal var taskGroup: ThrowingDiscardingTaskGroup<any Error>
    let registerCharacteristics:
        @Sendable (BluetoothUUID, borrowing BluetoothCharacteristicsWriter) async throws -> (UInt16, [UInt16])
    internal var characteristics = [Underlying]()
    
    public struct RegisteringCharacteristic<Value: Sendable>: Sendable {
        public var characteristic: BluetoothCharacteristic<Value>
        public var initialValue: Value
        public var withValues: @Sendable (ProduceEvents<Value>) async throws -> Void
        public var permissions: ATTAttributePermissions
        public var properties: GATTCharacteristicProperties
    }

    public mutating func add<each Value: Sendable>(
        serviceId: BluetoothUUID,
        characteristics: repeat RegisteringCharacteristic<each Value>
    ) async throws {
        for registration in repeat each characteristics {
            precondition(serviceId == registration.characteristic.serviceId, "ServiceID did not match that specified in the characteristic")
            
            let write = registration.characteristic.write(registration.initialValue)
            write { span in
                let underlying = Underlying(
                    uuid: registration.characteristic.id.uuid,
                    value: span.withUnsafeBytes { buffer in
                        Data(buffer)
                    },
                    permissions: .init(rawValue: registration.permissions.rawValue),
                    properties: .init(rawValue: registration.properties.rawValue),
                    descriptors: []  // TODO: Support
                )
                self.characteristics.append(underlying)
            }
            
            let (_, characteristicsHandles) = try await registerCharacteristics(registration.characteristic.serviceId, self)
            
            taskGroup.addTask { [peripheral] in
                guard characteristicsHandles.count == 1 else {
                    preconditionFailure("characteristicsHandles.count should have been 1, as 1 characteristic was registered")
                }
                
                let handle = characteristicsHandles[0]
                
                try await registration.withValues { event in
                    var data = Data()
                    let write = registration.characteristic.write(event)
                    
                    write { span in
                        data = span.withUnsafeBytes { buffer in
                            Data(buffer)
                        }
                    }
                    
                    try await peripheral.writeValue(
                        data,
                        to: handle
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
        properties: GATTCharacteristicProperties
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
            @Sendable (BluetoothUUID, borrowing BluetoothCharacteristicsWriter) async throws -> (UInt16, [UInt16])

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

    #if canImport(DarwinGATT)
        private static let peripheral = _Peripheral()
    #endif

    private let peripheral: _Peripheral
    private nonisolated let inboundWrapper = InboundWrapper()
    
    private final class InboundWrapper: @unchecked Sendable {
        var inbound: AsyncChannel<Data>?
        
        init() {}
    }

    internal init() async throws {
        #if canImport(DarwinGATT)
            self.peripheral = Self.peripheral

            while true {
                switch peripheral.state {
                case .unknown, .resetting, .unsupported, .unauthorized, .poweredOff:
                    // Wait to become active
                    try await Task.sleep(for: .seconds(1))
                case .poweredOn:
                    return
                }
            }
        #else
            guard let hostController = await HostController.default else {
                throw BluetoothNotAvailableError()
            }

            self.peripheral = _Peripheral(
                hostController: hostController,
                options: GATTPeripheralOptions(
                    maximumTransmissionUnit: .max,
                    maximumPreparedWrites: 1000
                ),
                socket: BluetoothLinux.L2CAPSocket.Server.self
            )
        #endif
        
        let logger = Logger(label: "engineer.edge.taps.bluetooth.peripheral")
        self.peripheral.log = { string in
            logger.error("\(string)")
        }
    }
    
    internal nonisolated func read(
        characteristicId: BluetoothUUID,
        into inbound: AsyncChannel<Data>
    ) {
        self.peripheral.willRead = { read in
            Task {
                await inbound.send(read.value)
            }
            return nil
        }
    }
    
    internal func writeValue(
        _ value: Data,
        to characteristicHandle: UInt16
    ) async throws {
        self.peripheral.write(value, forCharacteristic: characteristicHandle)
    }

    internal func run(
        localName: String?
    ) async throws {
        try await withTaskCancellationHandler {
            #if canImport(DarwinGATT)
                try await peripheral.start(
                    options: _Peripheral.AdvertisingOptions(localName: localName)
                )
            #else
                peripheral.start()
            #endif

            while true {
                try await Task.sleep(for: .seconds(100_000))
            }
        } onCancel: {
            peripheral.stop()
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
                ) { id, writer in
                    #if canImport(DarwinGATT)
                    try await self.peripheral.add(
                        service: GATTAttribute<Data>.Service(
                            uuid: id.uuid,
                            isPrimary: true,
                            characteristics: writer.characteristics,
                            includedServices: []
                        )
                    )
                    #else
                    self.peripheral.add(
                        service: GATTAttribute<Data>.Service(
                            uuid: id.uuid,
                            isPrimary: true,
                            characteristics: writer.characteristics,
                            includedServices: []
                        )
                    )
                    #endif
                }
                try await registerServices(&registration)
                
#if canImport(DarwinGATT)
                try await peripheral.start(
                    options: _Peripheral.AdvertisingOptions(
                        localName: localName,
                        serviceUUIDs: services.ids
                    )
                )
#else
                peripheral.start()
#endif
                
                try await gracefulShutdown()
                peripheral.stop()
            }
        } onCancel: {
            peripheral.stop()
        }
    }
}
