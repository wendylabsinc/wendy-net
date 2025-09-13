#if canImport(DarwinGATT)
    #if canImport(DarwinGATT)
        internal import Bluetooth
        internal import GATT
        internal import DarwinGATT
    #elseif canImport(BluetoothLinux)
        internal import Bluetooth
        internal import BluetoothLinux
    #endif

    #if canImport(FoundationEssentials)
        import FoundationEssentials
    #else
        import Foundation
    #endif

    extension BluetoothPeripheral {

    }
#endif
