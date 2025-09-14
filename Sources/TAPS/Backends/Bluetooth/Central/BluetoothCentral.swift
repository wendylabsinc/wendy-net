import AsyncAlgorithms
internal import Bluetooth
internal import GATT
import Logging
import ServiceLifecycle

#if canImport(DarwinGATT)
    internal import DarwinGATT

    internal typealias _BluetoothCentral = DarwinCentral
#elseif canImport(BluetoothLinux)
    internal import BluetoothLinux

    internal typealias _BluetoothCentral = GATTCentral<
        BluetoothLinux.HostController, BluetoothLinux.L2CAPSocket.Connection
    >
#endif

#if canImport(FoundationEssentials)
    import FoundationEssentials
#else
    import Foundation
#endif

public struct BluetoothService: Sendable, Identifiable {
    public let id: BluetoothUUID
    public let isPrimary: Bool
}

public struct BluetoothAdvertisement: @unchecked Sendable {
    public struct ServiceData: @unchecked Sendable {
        public let id: BluetoothUUID
        internal let data: Data

        init(id: BluetoothUUID, data: Data) {
            self.id = id
            self.data = data
        }

        #if canImport(BluetoothLinux)
            init(id: BluetoothUUID, data: LowEnergyAdvertisingData) {
                self.id = id
                self.data = data.withUnsafeData { data in
                    // Copy the data
                    Data(data)
                }
            }
        #endif

        #if swift(>=6.2)
            public func withServiceData<T, E: Error>(
                _ perform: (borrowing Span<UInt8>) throws(E) -> T
            ) throws(E) -> T {
                try perform(data.span)
            }
        #endif

        internal init(id: Bluetooth.BluetoothUUID, data: Data) {
            self.id = BluetoothUUID(uuid: id)
            self.data = data
        }
    }

    private let data: _BluetoothCentral.Advertisement

    internal init(data: _BluetoothCentral.Advertisement) {
        self.data = data
    }

    public var localName: String? { data.localName }
    public var serviceData: [ServiceData]? {
        data.serviceData?.map { id, data in
            ServiceData(id: BluetoothUUID(uuid: id), data: data)
        }
    }
    public var serviceUUIDs: [BluetoothUUID]? {
        data.serviceUUIDs?.map(BluetoothUUID.init)
    }
    public var solicitedServiceUUIDs: [BluetoothUUID]? {
        data.solicitedServiceUUIDs?.map(BluetoothUUID.init)
    }
}

public actor BluetoothCentral {
    public struct Peer: Sendable {
        internal let data: AsyncCentralScan<_BluetoothCentral>.Element
        public let name: String?

        public var discoveredAt: Date {
            data.date
        }
        public var isConnectable: Bool {
            data.isConnectable
        }
        // internal var id: UUID {
        //     #if canImport(DarwinGATT)
        //         return data.peripheral.id
        //     #elseif canImport(BluetoothLinux)
        //         return data.peripheral
        //     #endif
        // }
        public var rssi: RSSI? {
            RSSI(data.rssi)
        }
        public var advertisement: BluetoothAdvertisement {
            BluetoothAdvertisement(data: data.advertisementData)
        }
    }

    public actor Peripheral: Sendable, ServiceLifecycle.Service {
        nonisolated let peripheral: _BluetoothCentral.Peripheral
        nonisolated let central: BluetoothCentral

        internal init(peripheral: _BluetoothCentral.Peripheral, central: BluetoothCentral) {
            self.peripheral = peripheral
            self.central = central
        }

        public func run() async throws {
            try await gracefulShutdown()
        }
    }

    #if canImport(DarwinGATT)
        private static let central = _BluetoothCentral()
    #endif

    internal nonisolated let central: _BluetoothCentral
    private let inbound = AsyncChannel<_NetworkBytes>()

    internal init() async throws {
        #if canImport(DarwinGATT)
            self.central = Self.central
        #elseif canImport(BluetoothLinux)
            guard let hostController = await HostController.default else {
                throw BluetoothNotAvailableError()
            }

            self.central = _BluetoothCentral(
                hostController: hostController,
                socket: BluetoothLinux.L2CAPSocket.Connection.self
            )
        #endif

        #if canImport(DarwinGATT)
            while true {
                let state = await central.state
                switch state {
                case .unknown, .resetting, .unsupported, .unauthorized, .poweredOff:
                    // Wait to become active
                    try await Task.sleep(for: .seconds(1))
                case .poweredOn:
                    return
                }
            }
        #endif
        
        let logger = Logger(label: "engineer.edge.taps.bluetooth.peripheral")
        self.central.log = { string in
            logger.info("\(string)")
        }
    }

    public func listServices(for peer: Peer) async throws -> [BluetoothService] {
        let services = try await central.discoverServices(for: peer.data.peripheral)
        return services.map { service in
            BluetoothService(
                id: BluetoothUUID(uuid: service.uuid),
                isPrimary: service.isPrimary
            )
        }
    }

    internal func withConnection<T: Sendable>(
        _ peripheral: _BluetoothCentral.Peripheral,
        perform: (Peripheral) async throws -> T
    ) async throws -> T {
        try await central.connect(to: peripheral)

        do {
            let connected = Peripheral(
                peripheral: peripheral,
                central: self
            )

            let result = try await perform(connected)
            await central.disconnect(peripheral)
            return result
        } catch {
            await central.disconnect(peripheral)
            throw error
        }
    }
}
