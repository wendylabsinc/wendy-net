internal import Bluetooth
import Logging
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
    
    internal let peripheral: BluetoothPeripheral
    internal var taskGroup: ThrowingDiscardingTaskGroup<any Error>
    let registerCharacteristics:
        @Sendable (BluetoothUUID, borrowing BluetoothCharacteristicsWriter) async throws -> (UInt16, [UInt16])
    internal var characteristics = [Underlying]()

    public mutating func add<Service: BluetoothServiceProtocol>(
        service: Service,
        initialValue: Service.Value,
        permissions: ATTAttributePermissions,
        properties: GATTCharacteristicProperties
    ) async throws {
        let characteristic = Service.characteristic
        let write = characteristic.write(initialValue)
        write { span in
            let underlying = Underlying(
                uuid: Service.characteristic.id.uuid,
                value: span.withUnsafeBytes { buffer in
                    Data(buffer)
                },
                permissions: .init(rawValue: permissions.rawValue),
                properties: .init(rawValue: properties.rawValue),
                descriptors: []  // TODO: Support
            )
            self.characteristics.append(underlying)
        }
        
        let (_, characteristicsHandles) = try await registerCharacteristics(service.id, self)
        
        taskGroup.addTask { [peripheral, service] in
            guard characteristicsHandles.count == 1 else {
                preconditionFailure("characteristicsHandles.count should have been 1, as 1 characteristic was registered")
            }
            
            let handle = characteristicsHandles[0]
            
            try await service.withValues { event in
                var data = Data()
                let write = characteristic.write(event)
                
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

public protocol BluetoothServiceProtocol: Sendable, ~Copyable {
    associatedtype Value: Sendable
    
    var id: BluetoothUUID { get }
    static var characteristic: BluetoothCharacteristic<Value> { get }

    func writeCharacteristics(
        into writer: inout BluetoothCharacteristicsWriter
    ) async throws
    
    func withValues(_ events: (Value) async throws -> Void) async throws
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
            logger.info("\(string)")
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
