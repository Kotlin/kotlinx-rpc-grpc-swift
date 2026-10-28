// Copyright 2026 JetBrains s.r.o. and contributors.
// Use of this source code is governed by the Apache 2.0 license.

import Foundation
import XCTest

@testable import GrpcSwiftBridge

final class SwiftGrpcRequestBytesTests: XCTestCase {
    func testTransportReadsWrittenStorageWithoutCopy() {
        let storage = SwiftGrpcRequestBytes(count: 3)
        var writtenAddress: UnsafeMutableRawPointer?

        storage.withWritableBytes { pointer, count in
            XCTAssertEqual(count, 3)
            writtenAddress = pointer
            pointer!.storeBytes(of: UInt8(1), as: UInt8.self)
            pointer!.advanced(by: 1).storeBytes(of: UInt8(2), as: UInt8.self)
            pointer!.advanced(by: 2).storeBytes(of: UInt8(3), as: UInt8.self)
        }

        let message = storage.message
        XCTAssertEqual(message.withUnsafeBytes { Array($0) }, [1, 2, 3])
        XCTAssertEqual(message.withUnsafeBytes { $0.baseAddress }, UnsafeRawPointer(writtenAddress))
    }

    func testEmptyBufferSuppliesNullPointer() {
        let storage = SwiftGrpcRequestBytes(count: 0)
        storage.withWritableBytes { pointer, count in
            XCTAssertNil(pointer)
            XCTAssertEqual(count, 0)
        }
        XCTAssertEqual(storage.message.count, 0)
    }

    func testLargeMessageRetainsWrittenBytes() {
        let count = 4 * 1024 * 1024 - 1024
        let storage = SwiftGrpcRequestBytes(count: count)
        storage.withWritableBytes { pointer, capacity in
            XCTAssertEqual(capacity, count)
            let bytes = pointer!.assumingMemoryBound(to: UInt8.self)
            for index in 0..<count {
                bytes[index] = UInt8(truncatingIfNeeded: index)
            }
        }

        let message = storage.message
        XCTAssertEqual(message.count, count)
        XCTAssertTrue(
            message.withUnsafeBytes { buffer in
                buffer.enumerated().allSatisfy { index, byte in
                    byte == UInt8(truncatingIfNeeded: index)
                }
            })
    }
}
