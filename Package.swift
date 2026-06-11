// swift-tools-version: 6.3
import PackageDescription

let package = Package(
    name: "WendyNet",
    platforms: [
        .macOS(.v15),
    ],
    products: [
        .library(name: "WendyNet", targets: ["WendyNet"]),
    ],
    traits: [
        .trait(name: "Standard", description: "Desktop/server backend using SwiftNIO"),
        .trait(name: "WendyLite", description: "Embedded/WASM backend using WendyLite host imports"),
        .default(enabledTraits: ["Standard"]),
    ],
    dependencies: [
        .package(url: "https://github.com/wendylabsinc/wendy-lite.git", branch: "main"),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.50.0"),
    ],
    targets: [
        .target(
            name: "CWendyNet",
            path: "Sources/CWendyNet"
        ),
        .target(
            name: "WendyNet",
            dependencies: [
                .target(name: "CWendyNet", condition: .when(traits: ["WendyLite"])),
                .product(name: "WendyLite", package: "wendy-lite",
                         condition: .when(traits: ["WendyLite"])),
                .product(name: "NIOCore", package: "swift-nio",
                         condition: .when(traits: ["Standard"])),
                .product(name: "NIOPosix", package: "swift-nio",
                         condition: .when(traits: ["Standard"])),
            ],
            path: "Sources/WendyNet",
            swiftSettings: [
                .define("WendyNetBackendStandard", .when(traits: ["Standard"])),
                .define("WendyNetBackendWendyLite", .when(traits: ["WendyLite"])),
                .enableExperimentalFeature("Embedded", .when(traits: ["WendyLite"])),
                .unsafeFlags(["-wmo"], .when(traits: ["WendyLite"])),
                .treatAllWarnings(as: .error),
            ]
        ),
        .testTarget(
            name: "WendyNetTests",
            dependencies: ["WendyNet"],
            path: "Tests/WendyNetTests",
            swiftSettings: [
                .define("WendyNetBackendStandard", .when(traits: ["Standard"])),
                .define("WendyNetBackendWendyLite", .when(traits: ["WendyLite"])),
                .treatAllWarnings(as: .error),
            ]
        ),
    ]
)
