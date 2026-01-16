import Foundation
import Observation
import Synchronization
import TAPS

let taps = try await TAPS()
let name = "MyGatt"
// 40A74DF2-E238-421D-AE7F-7F3562E8FF6E
let testServiceId = BluetoothUUID(
  0x40, 0xA7, 0x4D, 0xF2, 0xE2, 0x38, 0x42, 0x1D,
  0xAE, 0x7F, 0x7F, 0x35, 0x62, 0xE8, 0xFF, 0x6E
)
// 40A74DF2-E238-421D-AE7F-7F3562E8FF6F
let testCharacteristicId = BluetoothUUID(
  0x40, 0xA7, 0x4D, 0xF2, 0xE2, 0x38, 0x42, 0x1D,
  0xAE, 0x7F, 0x7F, 0x35, 0x62, 0xE8, 0xFF, 0x6F
)
#if os(Linux)
  let isPeripheral = true
#else
  let isPeripheral = false
#endif

@Observable
final class Metrics: @unchecked Sendable {
  var batteryLevel = Characteristics.BatteryLevel(level: 82)
  var heartbeat: UInt64 = 255
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
            keyPath: \.batteryLevel,
            characteristic: .batteryLevel
          ),
          MetricsBluetoothService.Observation(
            keyPath: \.heartbeat,
            characteristic: .heartbeat
          )
        )
      )
    }

    group.addTask {
      while !Task.isCancelled {
        entity.heartbeat = .random(in: 40..<150)
        entity.batteryLevel = .init(level: .random(in: 0...100))
        try await Task.sleep(for: .seconds(1))
      }
    }
  } else {
    group.addTask {
      try await taps.withConnection(
        target: .bluetoothPeripheral(.named(name))
      ) { peripheral in
        print("Connected to \"\(name)\"")

        try await peripheral.observeCharacteristic(.fakeBatteryLevel) { batteryLevel in
          print(batteryLevel)
        }

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

  try await group.next()

  group.cancelAll()
}

public struct FakeBatteryService: BluetoothServiceProtocol {
  public var id: BluetoothUUID { testServiceId }
  public static var characteristic: BluetoothCharacteristic<Characteristics.BatteryLevel> {
    BluetoothCharacteristic<Characteristics.BatteryLevel>.fakeBatteryLevel
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
  public static let heartbeat = BluetoothCharacteristic(
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
