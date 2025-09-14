// swift-tools-version: 6.1
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "TAPS",
    platforms: [
        .macOS("26.0"),
        .iOS(.v18),
        .tvOS(.v18),
        .watchOS(.v10),
        .visionOS(.v1),
    ],
    products: [
        .library(
            name: "TAPS",
            targets: ["TAPS"]
        )
    ],
    dependencies: [
        // Private dependencies
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.86.0"),
        .package(url: "https://github.com/apple/swift-nio-extras.git", from: "1.29.0"),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", from: "2.34.0"),
        .package(url: "https://github.com/PureSwift/Bluetooth.git", from: "7.2.3"),
        .package(url: "https://github.com/PureSwift/GATT.git", from: "3.3.1"),
        .package(
            url: "https://github.com/PureSwift/BluetoothLinux.git",
            revision: "077f9d36afdae57e12c436842db56ef318e2fd23"),
        // Public dependencies
        .package(url: "https://github.com/swift-server/swift-service-lifecycle.git", from: "2.8.0"),
        .package(url: "https://github.com/apple/swift-http-types.git", from: "1.4.0"),
        .package(url: "https://github.com/apple/swift-async-algorithms.git", from: "1.0.0"),
        .package(url: "https://github.com/apple/swift-argument-parser", from: "1.4.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.4.0"),
        .package(url: "https://github.com/apple/swift-async-dns-resolver.git", from: "0.4.0"),
    ],
    targets: [
        .executableTarget(
            name: "TAPSBluetoothExample",
            dependencies: [
                "TAPS"
            ]),
        .target(
            name: "TAPS",
            dependencies: [
                // Private dependencies
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
                .product(name: "NIOExtras", package: "swift-nio-extras"),
                .product(name: "NIOHTTPCompression", package: "swift-nio-extras"),
                .product(name: "NIOHTTPTypes", package: "swift-nio-extras"),
                .product(name: "NIOHTTPTypesHTTP1", package: "swift-nio-extras"),
                .product(name: "AsyncDNSResolver", package: "swift-async-dns-resolver"),
                .product(name: "NIOSSL", package: "swift-nio-ssl"),
                .product(name: "Bluetooth", package: "Bluetooth"),
                .product(
                    name: "BluetoothLinux", package: "BluetoothLinux",
                    condition: .when(platforms: [.linux])),
                .product(
                    name: "GATT", package: "GATT",
                    condition: .when(platforms: [.macOS, .linux])),
                .product(
                    name: "DarwinGATT", package: "GATT", condition: .when(platforms: [.macOS])),
                // Public dependencies
                .product(name: "ServiceLifecycle", package: "swift-service-lifecycle"),
                .product(name: "HTTPTypes", package: "swift-http-types"),
                .product(name: "AsyncAlgorithms", package: "swift-async-algorithms"),
                .product(name: "Logging", package: "swift-log"),
            ]
        ),
        .testTarget(
            name: "TAPSTests",
            dependencies: ["TAPS"]
        ),
    ]
)
