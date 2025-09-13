import Foundation
import TAPS

let taps = try await TAPS()
let name = "EdgeOS Device"
let testServiceId = BluetoothUUID(uuid: UUID(uuidString: "40A74DF2-E238-421D-AE7F-7F3562E8FF6E")!)
let testCharacteristicId = BluetoothUUID(
    uuid: UUID(uuidString: "40A74DF2-E238-421D-AE7F-7F3562E8FF6F")!)
#if os(Linux)
    let isPeripheral = true
#else
    let isPeripheral = false
#endif

try await withThrowingTaskGroup { group in
    group.addTask {
        try await taps.run()
    }

    if isPeripheral {
        group.addTask {
            try await taps.withConnection(
                to: .bluetoothPeripheral(.named(name))
            ) { peripheral in
                print("Connected to \"\(name)\"")

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
                }

                try await peripheral.observeCharacteristic(.batteryLevel) { battery in
                    print(battery.level)
                }
            }
        }
    } else {
        group.addTask {
            try await taps.advertiseBluetooth(
                localName: name,
                services: FakeBatteryService()
            )
        }
    }

    group.addTask {
        // Cancel the example after 60s
        try await Task.sleep(for: .seconds(60))
    }

    try await group.next()

    group.cancelAll()
}

public struct FakeBatteryService: BluetoothServiceProtocol {
    public var id: BluetoothUUID { testServiceId }

    public func writeCharacteristics(
        into writer: inout BluetoothCharacteristicsWriter
    ) async throws {
        try writer.add(
            .batteryLevel,
            value: .init(level: 82),
            permissions: .read,
            properties: .read
        )
    }
}

struct CharacteristicParsingError: Error {}
extension BluetoothCharacteristic<Characteristics.BatteryLevel> {
    public static let fakeBtteryLevel = BluetoothCharacteristic(
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
            write(array.span)
        }
    }
}
