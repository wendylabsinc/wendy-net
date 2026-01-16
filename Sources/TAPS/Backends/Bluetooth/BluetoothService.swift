internal import Bluetooth

#if canImport(FoundationEssentials)
  internal import FoundationEssentials
#else
  internal import Foundation
#endif

public struct BluetoothUUID: Sendable, Hashable, CustomDebugStringConvertible {
  internal let uuid: Bluetooth.BluetoothUUID

  public var description: String { uuid.description }

  public var debugDescription: String {
    uuid.description
  }

  internal init(uuid: Bluetooth.BluetoothUUID) {
    self.uuid = uuid
  }

  /// Creates a 16-bit Bluetooth UUID (for standard services/characteristics)
  public init(bit16: UInt16) {
    self.uuid = .bit16(bit16)
  }

  /// Creates a 32-bit Bluetooth UUID
  public init(bit32: UInt32) {
    self.uuid = .bit32(bit32)
  }

  /// Creates a 128-bit Bluetooth UUID from raw bytes
  /// Bytes should be in big-endian order (network byte order)
  public init(
    _ byte0: UInt8, _ byte1: UInt8, _ byte2: UInt8, _ byte3: UInt8,
    _ byte4: UInt8, _ byte5: UInt8, _ byte6: UInt8, _ byte7: UInt8,
    _ byte8: UInt8, _ byte9: UInt8, _ byte10: UInt8, _ byte11: UInt8,
    _ byte12: UInt8, _ byte13: UInt8, _ byte14: UInt8, _ byte15: UInt8
  ) {
    let uuid = UUID(uuid: (
      byte0, byte1, byte2, byte3, byte4, byte5, byte6, byte7,
      byte8, byte9, byte10, byte11, byte12, byte13, byte14, byte15
    ))
    self.uuid = .bit128(uuid)
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
