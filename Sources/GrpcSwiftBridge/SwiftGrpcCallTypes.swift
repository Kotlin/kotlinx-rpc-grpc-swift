// Copyright 2026 JetBrains s.r.o. and contributors.
// Use of this source code is governed by the Apache 2.0 license.

import Foundation
import GRPCCore

/// The request/response streaming shape of a gRPC method.
@objc(SwiftGrpcMethodType)
public enum SwiftGrpcMethodType: Int, Sendable {
    case unary = 0
    case clientStreaming = 1
    case serverStreaming = 2
    case bidirectionalStreaming = 3
}

/// Compression applied to outbound messages for a call.
@objc(SwiftGrpcCompression)
public enum SwiftGrpcCompression: Int, Sendable {
    case none = 0
    case gzip = 1
}

/// Whether a call waits for a connection after a transient failure.
@objc(SwiftGrpcWaitForReady)
public enum SwiftGrpcWaitForReady: Int, Sendable {
    /// Uses grpc-swift's default behavior.
    case unspecified = 0
    /// Fails the call when a transient connection failure occurs.
    case disabled = 1
    /// Waits for a connection, subject to the call's deadline.
    case enabled = 2

    internal var callOption: Bool? {
        switch self {
        case .unspecified: nil
        case .disabled: false
        case .enabled: true
        }
    }
}

/// Canonical gRPC status codes.
@objc(SwiftGrpcStatusCode)
public enum SwiftGrpcStatusCode: Int, Sendable {
    case ok = 0
    case cancelled = 1
    case unknown = 2
    case invalidArgument = 3
    case deadlineExceeded = 4
    case notFound = 5
    case alreadyExists = 6
    case permissionDenied = 7
    case resourceExhausted = 8
    case failedPrecondition = 9
    case aborted = 10
    case outOfRange = 11
    case unimplemented = 12
    case `internal` = 13
    case unavailable = 14
    case dataLoss = 15
    case unauthenticated = 16
}

/// Receives metadata entries synchronously in their stored order, including duplicate keys.
@objc(SwiftGrpcMetadataVisitor)
public protocol SwiftGrpcMetadataVisitor: AnyObject {
    /// Receives one string entry.
    @objc(visitStringWithKey:value:)
    func visitString(key: String, value: String)

    /// Visits one binary entry. The pointer is valid only during this callback.
    @objc(visitBinaryWithKey:bytes:length:)
    func visitBinary(key: String, bytes: UnsafeRawPointer?, length: Int)
}

/// gRPC metadata that preserves duplicate keys across the Objective-C bridge.
/// Access must be sequential.
@objc(SwiftGrpcMetadata)
public final class SwiftGrpcMetadata: NSObject, @unchecked Sendable {
    var metadata: Metadata

    @objc public override init() {
        self.metadata = Metadata()
        super.init()
    }

    internal init(_ metadata: Metadata) {
        self.metadata = metadata
        super.init()
    }

    /// The number of entries, including entries with duplicate keys.
    @objc public var count: Int {
        metadata.count
    }

    /// Visits application metadata in order, including duplicate keys.
    /// Binary pointers are valid only during the callback.
    @objc(visitEntries:)
    public func visitEntries(_ visitor: any SwiftGrpcMetadataVisitor) {
        for (key, value) in metadata
        where
            Self.shouldPropagateMetadataEntry(forKey: key)
        {
            switch value {
            case .string(let string):
                if key.hasSuffix("-bin") {
                    let padding = String(repeating: "=", count: (4 - string.count % 4) % 4)
                    guard let binary = Data(base64Encoded: string + padding) else {
                        preconditionFailure("Invalid base64 value for binary metadata key \(key)")
                    }
                    binary.withUnsafeBytes { buffer in
                        visitor.visitBinary(key: key, bytes: buffer.baseAddress, length: buffer.count)
                    }
                } else {
                    visitor.visitString(key: key, value: string)
                }
            case .binary(let binary):
                binary.withUnsafeBytes { buffer in
                    visitor.visitBinary(key: key, bytes: buffer.baseAddress, length: buffer.count)
                }
            }
        }
    }

    private static func shouldPropagateMetadataEntry(forKey key: String) -> Bool {
        // HTTP/2 pseudo-headers describe transport state.
        !key.hasPrefix(":")
    }

    @objc(addStringValue:forKey:)
    public func addStringValue(_ value: String, forKey key: String) {
        metadata.addString(value, forKey: key)
    }

    /// Copies one binary value. `bytes` must point to `length` bytes until this method returns.
    @objc(addBinaryValue:length:forKey:)
    public func addBinaryValue(
        _ bytes: UnsafeRawPointer?,
        length: Int,
        forKey key: String
    ) {
        precondition(length >= 0, "Binary metadata length must be nonnegative")
        precondition(bytes != nil || length == 0, "Nonempty metadata requires bytes")
        precondition(key.hasSuffix("-bin"), "Binary metadata keys must end in -bin")

        let buffer = UnsafeRawBufferPointer(start: bytes, count: length)
        metadata.addBinary(Array(buffer), forKey: key)
    }
}

/// A pull-based source backed by a Kotlin request `Flow`.
///
/// For unary and server-streaming calls, grpc-swift pulls exactly once, and the source completes
/// that pull only after the flow has ended with exactly one request.
@objc(SwiftGrpcRequestSource)
public protocol SwiftGrpcRequestSource: AnyObject, Sendable {
    /// Asynchronously supplies the next message.
    ///
    /// `(nil, nil)` marks the end of the request flow. An error takes precedence over a value.
    /// Only one pull can be outstanding. Call `completion` exactly once for each pull.
    @objc(nextRequestWithCompletion:)
    func nextRequest(
        _ completion: @escaping @Sendable (SwiftGrpcRequestBytes?, NSError?) -> Void
    )

    /// Cancels request-flow collection and completes an outstanding pull.
    /// Later pulls must also complete with cancellation or end-of-stream.
    @objc func cancel()
}

/// Terminal gRPC status. A non-OK status is a normal call result, not a bridge error.
@objc(SwiftGrpcStatus)
public final class SwiftGrpcStatus: NSObject, @unchecked Sendable {
    @objc public let code: SwiftGrpcStatusCode
    @objc public let message: String?

    internal init(code: SwiftGrpcStatusCode, message: String?) {
        self.code = code
        self.message = message
        super.init()
    }
}

/// Base class for the strictly ordered response event stream.
@objc(SwiftGrpcCallEvent)
public class SwiftGrpcCallEvent: NSObject, @unchecked Sendable {
    fileprivate override init() {
        super.init()
    }
}

/// Initial response metadata. At most one headers event is emitted.
@objc(SwiftGrpcHeadersEvent)
public final class SwiftGrpcHeadersEvent: SwiftGrpcCallEvent, @unchecked Sendable {
    @objc public let headers: SwiftGrpcMetadata

    internal init(headers: SwiftGrpcMetadata) {
        self.headers = headers
        super.init()
    }
}

/// One serialized response message.
@objc(SwiftGrpcMessageEvent)
public final class SwiftGrpcMessageEvent: SwiftGrpcCallEvent, @unchecked Sendable {
    let message: MessageBytes

    internal init(message: MessageBytes) {
        self.message = message
    }

    @objc public var length: Int {
        message.count
    }

    /// Borrows the message buffer. The pointer is valid only inside `body`.
    @objc(withUnsafeBytes:)
    public func withUnsafeBytes(_ body: (UnsafeRawPointer?, Int) -> Void) {
        message.withUnsafeBytes { buffer in
            body(buffer.baseAddress, buffer.count)
        }
    }
}

/// The terminal event. It is emitted exactly once after all response messages.
@objc(SwiftGrpcClosedEvent)
public final class SwiftGrpcClosedEvent: SwiftGrpcCallEvent, @unchecked Sendable {
    @objc public let status: SwiftGrpcStatus
    @objc public let trailers: SwiftGrpcMetadata

    internal init(status: SwiftGrpcStatus, trailers: SwiftGrpcMetadata) {
        self.status = status
        self.trailers = trailers
    }
}
