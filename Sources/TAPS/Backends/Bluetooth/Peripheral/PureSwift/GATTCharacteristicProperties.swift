//
//  GATTCharacteristicProperty.swift
//  Bluetooth
//
//  Created by Alsey Coleman Miller on 11/7/24.
//

/// GATT Characteristic Properties Bitfield values
public struct GATTCharacteristicProperties: OptionSet, Hashable, Sendable {

  public var rawValue: UInt8

  public init(rawValue: UInt8) {
    self.rawValue = rawValue
  }
}

// MARK: - ExpressibleByIntegerLiteral

extension GATTCharacteristicProperties: ExpressibleByIntegerLiteral {

  public init(integerLiteral value: UInt8) {
    self.rawValue = value
  }
}

// MARK: - Options

extension GATTCharacteristicProperties {

  public static var broadcast: GATTCharacteristicProperties { 0x01 }
  public static var read: GATTCharacteristicProperties { 0x02 }
  public static var writeWithoutResponse: GATTCharacteristicProperties { 0x04 }
  public static var write: GATTCharacteristicProperties { 0x08 }
  public static var notify: GATTCharacteristicProperties { 0x10 }
  public static var indicate: GATTCharacteristicProperties { 0x20 }

  /// Characteristic supports write with signature
  public static var signedWrite: GATTCharacteristicProperties { 0x40 }  // BT_GATT_CHRC_PROP_AUTH

  public static var extendedProperties: GATTCharacteristicProperties { 0x80 }
}
