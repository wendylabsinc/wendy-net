import AsyncAlgorithms
internal import Bluetooth
internal import GATT
import Logging
internal import NIOCore
internal import NIOPosix

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

extension BluetoothCentral.Peripheral {
    public struct Service: Sendable, Identifiable {
        fileprivate let underlying:
            GATT.Service<_BluetoothCentral.Peripheral, _BluetoothCentral.AttributeID>
        public var id: BluetoothUUID { BluetoothUUID(uuid: underlying.uuid) }
        public var isPrimary: Bool { underlying.isPrimary }
    }

    public struct Characteristic: Sendable, Identifiable {
        fileprivate let underlying:
            GATT.Characteristic<_BluetoothCentral.Peripheral, _BluetoothCentral.AttributeID>
        public var id: BluetoothUUID { BluetoothUUID(uuid: underlying.uuid) }
    }

    public var isConnected: Bool {
        get async {
            await central.central.peripherals[self.peripheral] == true
        }
    }

    public var rssi: RSSI {
        get async throws {
            let rssi = try await self.central.central.rssi(for: peripheral).rawValue
            return RSSI(unchecked: rssi)
        }
    }

    public var services: [Service] {
        get async throws {
            let services = try await central.central.discoverServices(for: peripheral)
            return services.map { service in
                return Service(underlying: service)
            }
        }
    }

    public func listCharacteristics(for service: Service) async throws -> [Characteristic] {
        precondition(
            service.underlying.peripheral == peripheral,
            "Cannot find characteristics for different peripheral")

        let characteristics = try await central.central.discoverCharacteristics(
            for: service.underlying
        )
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

    #if swift(>=6.2)
        public func readNotifications(
            forCharacteristic characteristic: Characteristic,
            perform: (borrowing Span<UInt8>) async throws -> Void
        ) async throws {
            #if canImport(DarwinGATT)
            let notifications = try await self.central.central.notify(
                for: characteristic.underlying)
            #else
            let notifications = self.central.central.notify(
                for: characteristic.underlying)
            #endif
            for try await notification in notifications {
                try await perform(notification.span)
            }
        }

        public func writeNotification(
            forCharacteristic characteristic: Characteristic,
            _ span: borrowing Span<UInt8>
        ) async throws {
            let data = span.withUnsafeBytes { buffer in
                Data(buffer)
            }

            try await self.central.central.writeValue(
                data,
                for: characteristic.underlying
            )
        }

        internal func observeCharacteristic(
            _ characteristic: Characteristic,
            perform: (Data) async throws -> Void
        ) async throws {
            precondition(
                characteristic.underlying.peripheral == peripheral,
                "Cannot observe characteristics for different peripheral")

            while !Task.isCancelled {
                let value = try await central.central.readValue(for: characteristic.underlying)
                try await perform(value)
            }
        }

        public func observeCharacteristic<Value: Sendable>(
            _ characteristic: BluetoothCharacteristic<Value>,
            perform: (Value) async throws -> Void
        ) async throws {
            let services = try await central.central.discoverServices(
                [characteristic.serviceId.uuid],
                for: peripheral
            )

            guard services.count == 1 else {
                return
            }

            let characteristics = try await central.central.discoverCharacteristics(
                [],
                for: services[0]
            )

            guard characteristics.count == 1 else {
                return
            }

            try await observeCharacteristic(
                Characteristic(underlying: characteristics[0])
            ) { value in
                let value = try characteristic.parse(value.span)
                try await perform(value)
            }
        }
    #endif
}
