internal import Bluetooth
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

public struct BluetoothCharacteristicsWriter: Sendable, ~Copyable {
    #if canImport(DarwinGATT)
        internal typealias Underlying = GATT.Characteristic<
            _BluetoothCentral.Peripheral, _BluetoothCentral.AttributeID
        >
    #else
        internal typealias Underlying = GATTAttribute<Data>.Characteristic
    #endif

    internal var characteristics = [Underlying]()

    public mutating func add<Value: Sendable>(
        _ characteristic: BluetoothCharacteristic<Value>,
        value: Value,
        permissions: ATTAttributePermissions,
        properties: GATTCharacteristicProperties
    ) throws {
        let write = characteristic.write(value)
        write { span in
            let underlying = Underlying(
                uuid: characteristic.id.uuid,
                value: span.withUnsafeBytes { buffer in
                    Data(buffer)
                },
                permissions: .init(rawValue: permissions.rawValue),
                properties: .init(rawValue: properties.rawValue),
                descriptors: []  // TODO: Support
            )
            self.characteristics.append(underlying)
        }
    }
}

public protocol BluetoothServiceProtocol: Sendable, ~Copyable {
    var id: BluetoothUUID { get }

    func writeCharacteristics(
        into writer: inout BluetoothCharacteristicsWriter
    ) async throws
}

public actor BluetoothPeripheral {
    public struct ServiceRegistration: Sendable, ~Copyable {
        var services = [Bluetooth.BluetoothUUID]()
        let registerCharacteristics:
            @Sendable (BluetoothUUID, consuming BluetoothCharacteristicsWriter) async throws -> Void

        mutating func register(_ service: some BluetoothServiceProtocol) async throws {
            services.append(service.id.uuid)

            var writer = BluetoothCharacteristicsWriter()
            try await service.writeCharacteristics(into: &writer)
            try await registerCharacteristics(service.id, writer)
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
        registerServices: (inout ServiceRegistration) async throws -> Void
    ) async throws {
        try await withTaskCancellationHandler {
            var registration = ServiceRegistration { id, writer in
                let (_, _) = try await self.peripheral.add(
                    service: GATTAttribute<Data>.Service(
                        uuid: id.uuid,
                        isPrimary: true,
                        characteristics: writer.characteristics,
                        includedServices: []
                    )
                )
            }
            try await registerServices(&registration)

            #if canImport(DarwinGATT)
                try await peripheral.start(
                    options: _Peripheral.AdvertisingOptions(
                        localName: localName,
                        serviceUUIDs: registration.services
                    )
                )
            #else
                peripheral.start()
            #endif

            try await gracefulShutdown()
            peripheral.stop()
        } onCancel: {
            peripheral.stop()
        }
    }
}
