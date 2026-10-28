// Copyright 2026 JetBrains s.r.o. and contributors.
// Use of this source code is governed by the Apache 2.0 license.

import Foundation
import GRPCCore

/// A running gRPC call with serialized request and response messages.
///
/// - Wait for each ``nextEvent(_:)`` callback before requesting another event.
/// - A concurrent pull fails and leaves the accepted pull unchanged.
/// - Events arrive in `Headers? -> Message* -> Closed` order.
/// - Stop pulling after a closed event or an error from an accepted pull.
/// - Cancellation completes an outstanding pull with a cancellation error.
@objc(SwiftGrpcCall)
public final class SwiftGrpcCall: NSObject, @unchecked Sendable {
    private let mailbox: ResponseEventMailbox
    private let callTask: Task<Void, Never>
    private let requestSource: any SwiftGrpcRequestSource

    // Keeping the owner alive also keeps its connection task alive for the duration of the call.
    private let owner: SwiftGrpcClient

    internal init(
        owner: SwiftGrpcClient,
        descriptor: MethodDescriptor,
        options: CallOptions,
        metadata: Metadata,
        requestSource: any SwiftGrpcRequestSource,
        initialRequest: SwiftGrpcRequestBytes?
    ) {
        let mailbox = ResponseEventMailbox()
        let callRunner = CallRunner(
            client: owner.client,
            descriptor: descriptor,
            options: options,
            metadata: metadata,
            requestSource: requestSource,
            initialRequest: initialRequest,
            mailbox: mailbox
        )

        self.owner = owner
        self.mailbox = mailbox
        self.requestSource = requestSource
        self.callTask = Task {
            await callRunner.run()
        }
        super.init()
    }

    /// Requests one response event. `completion` runs exactly once with an event or an error.
    /// Non-OK gRPC statuses use ``SwiftGrpcClosedEvent``.
    ///
    /// The callback can run before this method returns, on a Swift concurrency executor,
    /// or on the thread that cancels the call.

    @objc(nextEventWithCompletion:)
    public func nextEvent(
        _ completion: @escaping @Sendable (SwiftGrpcCallEvent?, NSError?) -> Void
    ) {
        self.mailbox.nextEvent(completion)
    }

    /// Cancels the RPC and any outstanding request or event pull.
    ///
    /// Swift task cancellation doesn't carry a message, so `message` is informational only.
    @objc(cancelWithMessage:)
    public func cancel(message _: String?) {
        // Complete the outstanding pull before grpc-swift reports its cancellation status.
        self.mailbox.cancel()
        self.callTask.cancel()
        self.requestSource.cancel()
    }

    /// Cancels the RPC and calls `completion` after its task has stopped.
    ///
    /// The callback can run on any Swift concurrency executor.
    @objc(cancelAndWaitWithMessage:completion:)
    public func cancelAndWait(
        message: String?,
        completion: @escaping @Sendable () -> Void
    ) {
        let callTask = self.callTask
        self.cancel(message: message)
        Task {
            await callTask.value
            completion()
        }
    }

    deinit {
        self.mailbox.cancel()
        self.callTask.cancel()
        self.requestSource.cancel()
    }
}
