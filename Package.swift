// swift-tools-version: 6.3
// Copyright 2026 JetBrains s.r.o. and contributors.
// Use of this source code is governed by the Apache 2.0 license.

import PackageDescription

let package = Package(
    name: "kotlinx-rpc-grpc-swift",
    platforms: [
        .macOS(.v15),
        .iOS(.v18),
    ],
    products: [
        .library(name: "GrpcSwiftBridge", targets: ["GrpcSwiftBridge"])
    ],
    dependencies: [
        .package(url: "https://github.com/grpc/grpc-swift-2.git", exact: "2.4.3"),
        .package(url: "https://github.com/grpc/grpc-swift-nio-transport.git", exact: "2.9.2"),
    ],
    targets: [
        .target(
            name: "GrpcSwiftBridge",
            dependencies: [
                .product(name: "GRPCCore", package: "grpc-swift-2"),
                .product(name: "GRPCNIOTransportHTTP2TransportServices", package: "grpc-swift-nio-transport"),
            ]
        ),
        .testTarget(name: "GrpcSwiftBridgeTests", dependencies: ["GrpcSwiftBridge"]),
    ],
    swiftLanguageModes: [.v6]
)
