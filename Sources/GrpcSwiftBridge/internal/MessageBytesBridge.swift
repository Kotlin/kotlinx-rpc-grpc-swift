// Copyright 2026 JetBrains s.r.o. and contributors.
// Use of this source code is governed by the Apache 2.0 license.

import GRPCCore
import GRPCNIOTransportHTTP2TransportServices

internal typealias MessageBytes = GRPCNIOTransportBytes

/// A no-op serializer for the transport message bytes.
internal struct MessageBytesSerializer: MessageSerializer<MessageBytes> {
    func serialize<Bytes: GRPCContiguousBytes>(
        _ message: MessageBytes
    ) throws -> Bytes {
        message as! Bytes
    }
}

/// A no-op deserializer for the transport message bytes.
internal struct MessageBytesDeserializer: MessageDeserializer<MessageBytes> {
    func deserialize<Bytes: GRPCContiguousBytes>(
        _ bytes: Bytes
    ) throws -> MessageBytes {
        bytes as! MessageBytes
    }
}

extension SwiftGrpcRequestSource {

    internal func makeStreamingClientRequest() -> StreamingClientRequest<MessageBytes> {
        StreamingClientRequest<MessageBytes> { writer in
            defer { self.cancel() }

            while let request = try await self.nextRequestMessage() {
                try Task.checkCancellation()

                let message = request.message
                try await writer.write(message)
            }
        }
    }

    /// Sends the only request of a unary or server-streaming call.
    ///
    /// An `initialRequest` is sent without pulling.
    /// Without `initialRequest`, the source must validate that its input ended
    /// with exactly one request before supplying that request. This method makes no end-of-stream pull.
    internal func makeSingleClientRequest(
        initialRequest: SwiftGrpcRequestBytes?
    ) -> StreamingClientRequest<MessageBytes> {
        StreamingClientRequest<MessageBytes> { writer in
            defer { self.cancel() }

            var request = initialRequest
            if request == nil {
                request = try await self.nextRequestMessage()
            }

            if let request {
                try Task.checkCancellation()

                let message = request.message
                try await writer.write(message)
            }
        }
    }

    private func nextRequestMessage() async throws -> SwiftGrpcRequestBytes? {
        try await awaitSwiftGrpcCompletion(
            onCancellation: { self.cancel() },
            { self.nextRequest($0) }
        )
    }
}
