// Copyright 2026 JetBrains s.r.o. and contributors.
// Use of this source code is governed by the Apache 2.0 license.

import Foundation
import GRPCCore

/// Executes one grpc-swift call and produces the bridge's ordered response event stream.
internal struct CallRunner: Sendable {
    private let client: SwiftGrpcClient.Client
    private let descriptor: MethodDescriptor
    private let options: CallOptions
    private let metadata: Metadata
    private let requestSource: any SwiftGrpcRequestSource
    private let initialRequest: SwiftGrpcRequestBytes?
    private let mailbox: ResponseEventMailbox

    internal init(
        client: SwiftGrpcClient.Client,
        descriptor: MethodDescriptor,
        options: CallOptions,
        metadata: Metadata,
        requestSource: any SwiftGrpcRequestSource,
        initialRequest: SwiftGrpcRequestBytes?,
        mailbox: ResponseEventMailbox
    ) {
        self.client = client
        self.descriptor = descriptor
        self.options = options
        self.metadata = metadata
        self.requestSource = requestSource
        self.initialRequest = initialRequest
        self.mailbox = mailbox
    }

    internal func run() async {
        // A response can end before grpc-swift consumes the complete Kotlin request flow.
        // Cancelling the source also resumes any request pull still waiting on Kotlin.
        defer { self.requestSource.cancel() }

        // Turn the Kotlin request source into a grpc-swift-compatible client request. Kotlin picks
        // a single-request source for the same method types.
        var request =
            self.descriptor.type?.isRequestStreaming == false
            ? self.requestSource.makeSingleClientRequest(initialRequest: self.initialRequest)
            : self.requestSource.makeStreamingClientRequest()
        request.metadata = self.metadata

        do {
            try await self.client.bidirectionalStreaming(
                request: request,
                descriptor: self.descriptor,
                serializer: MessageBytesSerializer(),
                deserializer: MessageBytesDeserializer(),
                options: self.options
            ) { response in
                // The handler can run in a child task that grpc-swift cancels when the deadline
                // expires. Cancellation withdraws a pending event. The runner then publishes
                // the resulting RPCError as the closed event.
                switch response.accepted {
                case .success(let contents):
                    // Sends suspend until Kotlin requests the event.
                    // This preserves ordering and propagates backpressure into grpc-swift.
                    try await self.mailbox.send(
                        SwiftGrpcHeadersEvent(headers: SwiftGrpcMetadata(contents.metadata))
                    )

                    for try await part in contents.bodyParts {
                        switch part {
                        case .message(let message):
                            try await self.mailbox.send(SwiftGrpcMessageEvent(message: message))

                        case .trailingMetadata(let trailers):
                            // Successful trailers become the single terminal event.
                            try await self.mailbox.send(
                                SwiftGrpcClosedEvent(
                                    code: .ok,
                                    message: nil,
                                    trailers: trailers
                                )
                            )
                            return
                        }
                    }

                    // An accepted grpc-swift response must end in status/trailing metadata.
                    throw CallBridgeError.missingTerminalEvent

                case .failure(let error):
                    // Rejection is a normal gRPC result without a body, not a bridge failure.
                    try await self.mailbox.send(error.closedEvent)
                }
            }
        } catch let error as RPCError {
            // Non-OK gRPC statuses are closed events. Cancellation and deinitialization end the
            // mailbox before cancelling this task, so failed delivery needs no cleanup.
            guard !Task.isCancelled else { return }
            try? await self.mailbox.send(error.closedEvent)
        } catch is CancellationError {
            self.mailbox.cancel()
        } catch {
            guard !Task.isCancelled else { return }
            self.mailbox.fail(error as NSError)
        }
    }
}

extension RPCError {
    fileprivate var closedEvent: SwiftGrpcClosedEvent {
        SwiftGrpcClosedEvent(
            code: SwiftGrpcStatusCode(rawValue: self.code.rawValue) ?? .unknown,
            message: self.message,
            trailers: self.metadata
        )
    }
}

extension SwiftGrpcClosedEvent {
    fileprivate convenience init(
        code: SwiftGrpcStatusCode,
        message: String?,
        trailers: Metadata
    ) {
        self.init(
            status: SwiftGrpcStatus(code: code, message: message),
            trailers: SwiftGrpcMetadata(trailers)
        )
    }
}
