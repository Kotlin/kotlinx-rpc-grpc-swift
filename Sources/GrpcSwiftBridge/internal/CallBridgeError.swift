// Copyright 2026 JetBrains s.r.o. and contributors.
// Use of this source code is governed by the Apache 2.0 license.

import Foundation

internal enum CallBridgeError: LocalizedError, Sendable {
    case invalidEventPull
    case missingTerminalEvent

    internal var errorDescription: String? {
        switch self {
        case .invalidEventPull:
            "Only one event pull can be outstanding. Wait for its callback, and stop pulling after close."
        case .missingTerminalEvent:
            "The gRPC response stream ended without a terminal event."
        }
    }
}
