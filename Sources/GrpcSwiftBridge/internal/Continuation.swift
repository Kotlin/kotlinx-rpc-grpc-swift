// Copyright 2026 JetBrains s.r.o. and contributors.
// Use of this source code is governed by the Apache 2.0 license.

import Foundation

/// A completion runs exactly once. An error takes precedence over a value.
/// A nil value with no error marks the end of a stream.
internal typealias SwiftGrpcCompletion<Value: Sendable> =
    @Sendable (
        _ value: Value?,
        _ error: NSError?
    ) -> Void

/// The cancellation handler must complete a registered callback and any callback registered later.
/// Checked continuations don't automatically resume when their task is cancelled.
internal func awaitSwiftGrpcCompletion<Value: Sendable>(
    onCancellation: @escaping @Sendable () -> Void,
    _ register: (_ completion: @escaping SwiftGrpcCompletion<Value>) -> Void
) async throws -> Value? {
    try await withTaskCancellationHandler {
        try await withCheckedThrowingContinuation { continuation in
            register { value, error in
                if let error {
                    continuation.resume(throwing: error)
                } else {
                    continuation.resume(returning: value)
                }
            }
        }
    } onCancel: {
        onCancellation()
    }
}
