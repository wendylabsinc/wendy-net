import Foundation
import TAPS
import Observation
import Synchronization

let taps = try await TAPS()
let name = "MyGatt"
let testServiceId = BluetoothUUID(uuid: UUID(uuidString: "40A74DF2-E238-421D-AE7F-7F3562E8FF6E")!)
let testCharacteristicId = BluetoothUUID(
    uuid: UUID(uuidString: "40A74DF2-E238-421D-AE7F-7F3562E8FF6F")!)
#if os(Linux)
    let isPeripheral = true
#else
    let isPeripheral = true
#endif


@Observable
final class Metrics: @unchecked Sendable {
    var secondsAgo: UInt64 = 0
}

try await withThrowingTaskGroup { group in
    group.addTask {
        try await taps.run()
    }

    if isPeripheral {
        let entity = Metrics()
        group.addTask {
            try await taps.advertiseBluetooth(
                localName: name,
                services: MetricsBluetoothService(
                    serviceId: testServiceId,
                    observing: entity,
                    observations: MetricsBluetoothService.Observation(
                        keyPath: \.secondsAgo,
                        characteristic: .seconds
                    )
                )
            )
        }
        
        group.addTask {
            while !Task.isCancelled {
                entity.secondsAgo += 1
                try await Task.sleep(for: .seconds(1))
            }
        }
        
//        group.addTask {
//            while !Task.isCancelled {
//                do {
//                    try await taps.withConnection(
//                        to: .gattCentral()
//                    ) { connection in
//                        print("Received connection")
////                        Task {
////                            if #available(macOS 26.0, *) {
////                                for i in 0...255 {
////                                    try await connection.send(NetworkOutputBytes(string: "Hello \(i)"))
////                                    try await Task.sleep(for: .seconds(1))
////                                }
////                            }
////                        }
//                        
//                        try await connection.withEachMessage { span in
//                            span.withUnsafeBytes { buffer in
//                                print(Array(buffer))
//                            }
//                        }
//                    }
//                } catch {
//                    print(error)
//                }
//            }
//        }
    } else {
        group.addTask {
            try await taps.withConnection(
                to: .bluetoothPeripheral(.named(name))
            ) { peripheral in
                print("Connected to \"\(name)\"")

                // GATT
                guard
                    let characteristic = try await peripheral.getCharacteristic(
                        testCharacteristicId,
                        forService: testServiceId
                    )
                else {
                    return
                }

                if #available(macOS 26.0, *) {
                    var data = [UInt8]()
                    for i: UInt8 in 0...255 {
                        data.append(i)
                        try await peripheral.writeNotification(
                            forCharacteristic: characteristic,
                            data.span
                        )
                    }
                    try await Task.sleep(for: .milliseconds(100))
                }
            }
        }
    }

//    group.addTask {
//        // Cancel the example after 60s
//        try await Task.sleep(for: .seconds(60))
//    }

    try await group.next()

    group.cancelAll()
}

public struct FakeBatteryService: BluetoothServiceProtocol {
    public var id: BluetoothUUID { testServiceId }
    public static var characteristic: BluetoothCharacteristic<Characteristics.BatteryLevel> {
        BluetoothCharacteristic.fakeBatteryLevel
    }

    public func writeCharacteristics(
        into writer: inout BluetoothCharacteristicsWriter
    ) async throws {
        try await writer.add(
            characteristic: Self.characteristic,
            initialValue: .init(level: 82),
            withValues: { handle in
                while !Task.isShuttingDownGracefully && !Task.isCancelled {
                    try await handle(.init(level: 82))
                    try await Task.sleep(for: .seconds(1))
                }
            },
            permissions: .read,
            properties: .read
        )
    }
}

struct CharacteristicParsingError: Error {}
extension BluetoothCharacteristic<Characteristics.BatteryLevel> {
    public static let fakeBatteryLevel = BluetoothCharacteristic(
        id: testCharacteristicId,
        serviceId: testServiceId
    ) { span in
        guard
            span.count == 1,
            0...100 ~= span[0]
        else {
            throw CharacteristicParsingError()
        }

        return Characteristics.BatteryLevel(level: span[0])
    } write: { value in
        return { write in
            let array = InlineArray<1, UInt8>(repeating: value.level)
            write(array.span.bytes)
        }
    }
}

extension BluetoothCharacteristic<UInt64> {
    public static let seconds = BluetoothCharacteristic(
        id: testCharacteristicId,
        serviceId: testServiceId
    ) { span in
        guard span.count == 8 else {
            throw CharacteristicParsingError()
        }

        return span.bytes.unsafeLoad(as: UInt64.self)
    } write: { value in
        return { write in
            let array = InlineArray<1, UInt64>(repeating: value)
            write(array.span.bytes)
        }
    }
}
