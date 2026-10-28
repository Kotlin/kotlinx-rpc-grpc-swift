// Copyright 2026 JetBrains s.r.o. and contributors.
// Use of this source code is governed by the Apache 2.0 license.

import Foundation
import Synchronization

/// Delivers response events to `nextEvent` callbacks, one event at a time.
internal final class ResponseEventMailbox: Sendable {
    internal typealias Completion = @Sendable (SwiftGrpcCallEvent?, NSError?) -> Void

    private enum State {
        case idle
        case waitingPull(Completion)
        case offered(SwiftGrpcCallEvent, UnsafeContinuation<Void, any Error>)
        case terminalPending(NSError)
        case terminalDelivered
    }

    private enum PullAction {
        case wait
        case reject
        case deliver(SwiftGrpcCallEvent, UnsafeContinuation<Void, any Error>)
        case fail(NSError)
    }

    private enum SendAction {
        case deliver(Completion)
        case offer
        case reject
    }

    private enum OfferAction {
        case deliver(Completion)
        case stored
        case reject
    }

    private enum TerminateAction {
        case none
        case fail(Completion)
        case release(UnsafeContinuation<Void, any Error>)
    }

    private let state = Mutex<State>(.idle)

    internal init() {}

    internal var hasPendingEvent: Bool {
        self.state.withLock { state in
            if case .offered = state {
                return true
            }
            return false
        }
    }

    /// Register a callback that is called when a new event is sent.
    internal func nextEvent(_ completion: @escaping Completion) {
        let action = self.state.withLock { state -> PullAction in
            switch state {
            case .idle:
                state = .waitingPull(completion)
                return .wait

            case .waitingPull, .terminalDelivered:
                return .reject

            case .offered(let event, let ack):
                state = event.isTerminal ? .terminalDelivered : .idle
                return .deliver(event, ack)

            case .terminalPending(let error):
                state = .terminalDelivered
                return .fail(error)
            }
        }

        // Run callbacks outside the lock: they can request another event or cancel the call.
        switch action {
        case .wait:
            break

        case .reject:
            completion(nil, CallBridgeError.invalidEventPull as NSError)

        case .deliver(let event, let ack):
            // Resume the producer after the callback returns, so a reentrant pull registers first.
            completion(event, nil)
            ack.resume()

        case .fail(let error):
            completion(nil, error)
        }
    }

    /// Waits until a pull claims `event`. A cancelled producer withdraws its unclaimed event
    /// and throws `CancellationError` but leaves the mailbox open for a final status.
    internal func send(_ event: SwiftGrpcCallEvent) async throws {
        try Task.checkCancellation()

        // A waiting pull doesn't need a continuation.
        let action = self.state.withLock { state -> SendAction in
            switch state {
            case .waitingPull(let completion):
                state = event.isTerminal ? .terminalDelivered : .idle
                return .deliver(completion)

            case .idle:
                return .offer

            case .offered:
                preconditionFailure("The response producer made a second offer before the first was claimed")

            case .terminalPending, .terminalDelivered:
                return .reject
            }
        }

        switch action {
        case .deliver(let completion):
            completion(event, nil)

        case .reject:
            throw CancellationError()

        case .offer:
            try await self.offer(event)
        }

        try Task.checkCancellation()
    }

    private func offer(_ event: SwiftGrpcCallEvent) async throws {
        try await withTaskCancellationHandler {
            try await withUnsafeThrowingContinuation { (ack: UnsafeContinuation<Void, any Error>) in
                // A pull or cancellation can arrive between the two lock acquisitions.
                let action = self.state.withLock { state -> OfferAction in
                    switch state {
                    case .waitingPull(let completion):
                        state = event.isTerminal ? .terminalDelivered : .idle
                        return .deliver(completion)

                    case .idle:
                        // Cancellation can precede this offer. Its handler then has nothing to withdraw.
                        if Task.isCancelled {
                            return .reject
                        }
                        state = .offered(event, ack)
                        return .stored

                    case .offered:
                        preconditionFailure(
                            "The response producer made a second offer before the first was claimed")

                    case .terminalPending, .terminalDelivered:
                        return .reject
                    }
                }

                switch action {
                case .deliver(let completion):
                    completion(event, nil)
                    ack.resume()

                case .stored:
                    break

                case .reject:
                    ack.resume(throwing: CancellationError())
                }
            }
        } onCancel: {
            // A deadline can cancel grpc-swift's response handler. Keep the mailbox open
            // so the runner can still send the final status.
            self.withdraw()
        }
    }

    /// Withdraws an unclaimed event. The producer can still send a final status.
    internal func withdraw() {
        let ack = self.state.withLock { state -> UnsafeContinuation<Void, any Error>? in
            guard case .offered(_, let ack) = state else {
                return nil
            }
            state = .idle
            return ack
        }
        ack?.resume(throwing: CancellationError())
    }

    internal func cancel() {
        self.fail(CancellationError() as NSError)
    }

    /// Delivers `error` to the waiting or next pull. An unclaimed event is removed,
    /// and its producer's ``send(_:)`` throws. The first terminal result wins.
    internal func fail(_ error: NSError) {
        let action = self.state.withLock { state -> TerminateAction in
            switch state {
            case .idle:
                state = .terminalPending(error)
                return .none

            case .waitingPull(let completion):
                state = .terminalDelivered
                return .fail(completion)

            case .offered(_, let ack):
                state = .terminalPending(error)
                return .release(ack)

            case .terminalPending, .terminalDelivered:
                return .none
            }
        }

        switch action {
        case .none:
            break

        case .fail(let completion):
            completion(nil, error)

        case .release(let ack):
            ack.resume(throwing: CancellationError())
        }
    }
}

extension SwiftGrpcCallEvent {
    fileprivate var isTerminal: Bool {
        self is SwiftGrpcClosedEvent
    }
}
