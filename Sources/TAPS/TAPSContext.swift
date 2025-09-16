/// Context type for TAPS framework operations
///
/// This is a placeholder struct for future framework improvements.
/// It cannot be publicly initialized as it's intended for framework use only.
public struct TAPSContext: Sendable {
  internal let bluetoothPeripheral: BluetoothPeripheral
  internal let bluetoothCentral: BluetoothCentral
}
