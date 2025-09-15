public struct BluetoothCharacteristic<Value: Sendable>: Sendable {
    public let serviceId: BluetoothService.ID
    public let id: BluetoothUUID

    public typealias WithValue = (borrowing RawSpan) -> Void

    internal let parse: @Sendable (borrowing Span<UInt8>) throws -> Value
    internal let write: @Sendable (Value) -> (WithValue) -> Void

    public init(
        id: BluetoothUUID,
        serviceId: BluetoothUUID,
        parse: @Sendable @escaping (Span<UInt8>) throws -> Value,
        write: @Sendable @escaping (Value) -> (WithValue) -> Void
    ) {
        self.serviceId = serviceId
        self.id = id
        self.parse = parse
        self.write = write
    }
}

public enum Characteristics {
    public struct BatteryLevel: Sendable {
        public let level: UInt8

        public init(level: UInt8) {
            precondition(
                0...100 ~= level, "Battery level out of bounds. Must be in range of 0 through 100")
            self.level = level
        }
    }
    
    public struct LocalName: Sendable {
        public let name: String
        
        public init(name: String) {
            self.name = name
        }
    }
}

public enum BluetoothServices {
    public static let battery = BluetoothUUID(uuid: .bit16(0x180f))
    public static let genericAccess = BluetoothUUID(uuid: .bit16(0x1800))
}

extension BluetoothCharacteristic<Characteristics.BatteryLevel> {
    public static let batteryLevel = BluetoothCharacteristic(
        id: BluetoothUUID(uuid: .bit16(0x2a19)),
        serviceId: BluetoothServices.battery
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

extension BluetoothCharacteristic<Characteristics.LocalName> {
    public static let localName = BluetoothCharacteristic(
        id: BluetoothUUID(uuid: .bit16(0x2a00)),
        serviceId: BluetoothServices.genericAccess
    ) { span in
        let span = try UTF8Span(validating: span)
        let name = String(copying: span)
        return Characteristics.LocalName(name: name)
    } write: { value in
        return { write in
            write(value.name.utf8Span.span.bytes)
        }
    }
}
