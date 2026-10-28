// Copyright 2026 JetBrains s.r.o. and contributors.
// Use of this source code is governed by the Apache 2.0 license.

import Foundation
import GRPCCore
import XCTest

@testable import GrpcSwiftBridge

final class SwiftGrpcMetadataTests: XCTestCase {
    func testVisitsDuplicateValuesInOrder() {
        let metadata = SwiftGrpcMetadata()
        metadata.addStringValue("first", forKey: "x-label")
        [UInt8(1), 2, 3].withUnsafeBytes { buffer in
            metadata.addBinaryValue(buffer.baseAddress, length: buffer.count, forKey: "data-bin")
        }
        metadata.addStringValue("second", forKey: "x-label")

        let visitor = RecordingVisitor()
        metadata.visitEntries(visitor)

        XCTAssertEqual(
            visitor.entries,
            [
                .string("x-label", "first"),
                .binary("data-bin", [1, 2, 3]),
                .string("x-label", "second"),
            ])
    }

    func testDecodesPaddedAndUnpaddedBinaryStrings() {
        var wireMetadata = Metadata()
        wireMetadata.addString("AQI=", forKey: "data-bin")
        wireMetadata.addString("AwQ", forKey: "data-bin")
        let metadata = SwiftGrpcMetadata(wireMetadata)
        let visitor = RecordingVisitor()

        metadata.visitEntries(visitor)

        XCTAssertEqual(
            visitor.entries,
            [
                .binary("data-bin", [1, 2]),
                .binary("data-bin", [3, 4]),
            ])
    }

    func testEmptyBinaryValueAcceptsNullPointer() {
        let metadata = SwiftGrpcMetadata()
        metadata.addBinaryValue(nil, length: 0, forKey: "empty-bin")
        let visitor = RecordingVisitor()

        metadata.visitEntries(visitor)

        XCTAssertEqual(visitor.entries, [.binary("empty-bin", [])])
    }

    func testDoesNotExposeHttp2PseudoHeaders() {
        var wireMetadata = Metadata()
        wireMetadata.addString("200", forKey: ":status")
        wireMetadata.addString("value", forKey: "x-label")
        let metadata = SwiftGrpcMetadata(wireMetadata)
        let visitor = RecordingVisitor()

        metadata.visitEntries(visitor)

        XCTAssertEqual(visitor.entries, [.string("x-label", "value")])
    }
}

private final class RecordingVisitor: NSObject, SwiftGrpcMetadataVisitor {
    enum Entry: Equatable {
        case string(String, String)
        case binary(String, [UInt8])
    }

    var entries: [Entry] = []

    func visitString(key: String, value: String) {
        entries.append(.string(key, value))
    }

    func visitBinary(key: String, bytes: UnsafeRawPointer?, length: Int) {
        entries.append(.binary(key, Array(UnsafeRawBufferPointer(start: bytes, count: length))))
    }
}
