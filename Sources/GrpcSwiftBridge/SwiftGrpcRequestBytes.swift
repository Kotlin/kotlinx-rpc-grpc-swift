// Copyright 2026 JetBrains s.r.o. and contributors.
// Use of this source code is governed by the Apache 2.0 license.

import Foundation

/// Fixed-size request storage that Kotlin fills before grpc-swift reads it.
///
/// The caller must finish all writes before sending the request. Access must be sequential.
@objc(SwiftGrpcRequestBytes)
public final class SwiftGrpcRequestBytes: NSObject, @unchecked Sendable {
    internal private(set) var message: RawMessage

    /// The buffer size in bytes.
    @objc public let length: Int

    /// Allocates a zero-filled buffer. `count` must be nonnegative.
    @objc public init(count: Int) {
        precondition(count >= 0, "Request length must be nonnegative")
        self.length = count
        self.message = RawMessage(repeating: 0, count: count)
        super.init()
    }

    /// Borrows the storage for a synchronous write. The pointer is valid only inside `body`.
    /// An empty buffer supplies a null pointer.
    @objc(withWritableBytes:)
    public func withWritableBytes(_ body: (UnsafeMutableRawPointer?, Int) -> Void) {
        self.message.withUnsafeMutableBytes { buffer in
            body(buffer.isEmpty ? nil : buffer.baseAddress, buffer.count)
        }
    }
}
