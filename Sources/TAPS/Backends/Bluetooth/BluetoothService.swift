internal import Bluetooth

#if canImport(FoundationEssentials)
  import FoundationEssentials
#else
  import Foundation
#endif

public struct BluetoothUUID: Sendable, Hashable, CustomDebugStringConvertible {
  internal let uuid: Bluetooth.BluetoothUUID

  public var description: String { uuid.rawValue }

  public var debugDescription: String {
    uuid.description
  }

  internal init(uuid: Bluetooth.BluetoothUUID) {
    self.uuid = uuid
  }

  public init() {
    self.uuid = Bluetooth.BluetoothUUID()
  }

  public init(uuid: UUID) {
    self.uuid = Bluetooth.BluetoothUUID(uuid: uuid)
  }
}

public struct RSSI: Sendable {
  public let rawValue: Int8

  internal init(unchecked: Int8) {
    self.rawValue = unchecked
  }

  public init?(_ rawValue: Int8) {
    guard -127 <= rawValue, rawValue <= +20 else { return nil }

    self.rawValue = rawValue
  }

  public init?(_ rawValue: Double) {
    guard -127 <= rawValue, rawValue <= +20 else { return nil }

    self.rawValue = Int8(clamping: Int(rawValue))
  }
}
