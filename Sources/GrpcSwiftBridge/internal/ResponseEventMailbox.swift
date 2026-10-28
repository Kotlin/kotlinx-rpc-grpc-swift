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
        case offered(SwiftGrpcCallEvent, CheckedContinuation<Void, any Error>)
        case terminalPending(NSError)
        case terminalDelivered
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

    /// Requests one event. `completion` receives an event or an error exactly once.
    ///
    /// The callback can run before this method returns. Only one pull can wait at a time.
    internal func nextEvent(_ completion: @escaping Completion) {
        let actionOutsideOfLock = self.state.withLock { state -> () -> Void in
            switch state {
            case .idle:
                state = .waitingPull(completion)
                return {}

            case .waitingPull, .terminalDelivered:
                return { completion(nil, CallBridgeError.invalidEventPull as NSError) }

            case .offered(let event, let continuation):
                state = event.isTerminal ? .terminalDelivered : .idle
                return {
                    // Resume the producer after the callback returns, so a reentrant pull registers first.
                    completion(event, nil)
                    continuation.resume()
                }

            case .terminalPending(let error):
                state = .terminalDelivered
                return { completion(nil, error) }
            }
        }

        // Run callbacks outside the lock: they can request another event or cancel the call.
        actionOutsideOfLock()
    }

    /// Waits until a consumer accepts `event`. Only one send can be outstanding.
    ///
    /// Cancelling the producer removes an event that no consumer has accepted and leaves the mailbox open.
    /// If cancellation races with delivery, this method can throw `CancellationError`
    /// after the consumer accepts the event.
    internal func send(_ event: SwiftGrpcCallEvent) async throws {
        try Task.checkCancellation()

        // A waiting pull doesn't need a continuation.
        let previousState = self.state.withLock { state in
            let savedState = state
            if case .waitingPull = state {
                state = event.isTerminal ? .terminalDelivered : .idle
            }
            return savedState
        }

        switch previousState {
        case .waitingPull(let completion):
            completion(event, nil)

        case .terminalPending, .terminalDelivered:
            throw CancellationError()

        case .idle:
            try await self.offer(event)

        case .offered:
            preconditionFailure("The response producer made a second offer before the first was claimed")
        }

        try Task.checkCancellation()
    }

    /// Offers the event to a pull of `nextEvent`.
    /// If no pull is waiting, it sets the state to `.offered` with the continuation
    /// of this call's continuation object. In this case, it returns only after `nextEvent` takes
    /// the offer and resumes the continuation.
    private func offer(_ event: SwiftGrpcCallEvent) async throws {
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                // A pull or cancellation can arrive between the two lock acquisitions.
                let actionOutsideOfLock = self.state.withLock { state -> () -> Void in
                    switch state {
                    case .waitingPull(let completion):
                        state = event.isTerminal ? .terminalDelivered : .idle
                        return {
                            // Run completion outside of state lock.
                            completion(event, nil)
                            continuation.resume()
                        }

                    case .idle:
                        // Cancellation can precede this offer. Its handler then has nothing to withdraw.
                        if Task.isCancelled {
                            return { continuation.resume(throwing: CancellationError()) }
                        }
                        state = .offered(event, continuation)
                        return {}

                    case .offered:
                        preconditionFailure(
                            "The response producer made a second offer before the first was claimed")

                    case .terminalPending, .terminalDelivered:
                        return { continuation.resume(throwing: CancellationError()) }
                    }
                }

                actionOutsideOfLock()
            }
        } onCancel: {
            // A deadline can cancel grpc-swift's response handler. Keep the mailbox open
            // so the runner can still send the final status.
            self.withdraw()
        }
    }

    /// Withdraws an unclaimed event. The producer can still send a final status.
    private func withdraw() {
        let continuation = self.state.withLock { state -> CheckedContinuation<Void, any Error>? in
            guard case .offered(_, let continuation) = state else {
                return nil
            }
            state = .idle
            return continuation
        }
        continuation?.resume(throwing: CancellationError())
    }

    internal func cancel() {
        self.fail(CancellationError() as NSError)
    }

    /// Delivers `error` to the waiting or next pull. An unclaimed event is removed,
    /// and its producer's ``send(_:)`` throws. The first terminal result wins.
    internal func fail(_ error: NSError) {
        let actionOutsideOfLock = self.state.withLock { state -> () -> Void in
            switch state {
            case .idle:
                state = .terminalPending(error)
                return {}

            case .waitingPull(let completion):
                state = .terminalDelivered
                return { completion(nil, error) }

            case .offered(_, let continuation):
                state = .terminalPending(error)
                return { continuation.resume(throwing: CancellationError()) }

            case .terminalPending, .terminalDelivered:
                return {}
            }
        }

        actionOutsideOfLock()
    }
}

extension SwiftGrpcCallEvent {
    fileprivate var isTerminal: Bool {
        self is SwiftGrpcClosedEvent
    }
}
