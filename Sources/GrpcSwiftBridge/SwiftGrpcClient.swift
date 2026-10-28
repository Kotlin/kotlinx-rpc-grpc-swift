// Copyright 2026 JetBrains s.r.o. and contributors.
// Use of this source code is governed by the Apache 2.0 license.

import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2TransportServices

/// Configuration used to create a ``SwiftGrpcClient``.
///
/// A negative `keepAliveTimeMilliseconds` disables keepalive. The remaining keepalive values are
/// only used when keepalive is enabled.
@objc(SwiftGrpcClientConfiguration)
public final class SwiftGrpcClientConfiguration: NSObject {
    /// The DNS hostname to connect to.
    @objc public let host: String

    /// The TCP port to connect to.
    @objc public let port: Int

    /// Whether to use an unencrypted connection instead of TLS with the system trust store.
    @objc public let plaintext: Bool

    /// Overrides both the HTTP/2 `:authority` pseudo-header and the TLS SNI hostname.
    @objc public var overrideAuthority: String?

    /// The complete user-agent value added to calls, if set.
    @objc public var userAgent: String?

    /// The idle connection timeout in milliseconds. A negative value disables the timeout.
    @objc public var idleTimeoutMilliseconds: Int64 = 30 * 60 * 1_000

    /// The keepalive interval in milliseconds, or a negative value to disable keepalive.
    @objc public var keepAliveTimeMilliseconds: Int64 = -1

    /// The time to wait for a keepalive acknowledgement, in milliseconds.
    @objc public var keepAliveTimeoutMilliseconds: Int64 = 20_000

    /// Whether keepalive pings can be sent while there are no active calls.
    @objc public var keepAliveWithoutCalls: Bool = false

    @objc(initWithHost:port:plaintext:)
    public init(host: String, port: Int, plaintext: Bool) {
        self.host = host
        self.port = port
        self.plaintext = plaintext
        super.init()
    }
}

/// A long-lived grpc-swift client using Apple's Network.framework transport.
@objc(SwiftGrpcClient)
public final class SwiftGrpcClient: NSObject, @unchecked Sendable {
    internal typealias Transport = HTTP2ClientTransport.TransportServices
    internal typealias Client = GRPCClient<Transport>

    internal let client: Client
    internal let userAgent: String?

    private let connectionTask: Task<Void, Never>

    @objc(initWithConfiguration:error:)
    public init(configuration: SwiftGrpcClientConfiguration) throws {
        var transportConfiguration = Transport.Config.defaults
        transportConfiguration.http2.authority = configuration.overrideAuthority
        transportConfiguration.connection.maxIdleTime =
            configuration.idleTimeoutMilliseconds >= 0
            ? .milliseconds(configuration.idleTimeoutMilliseconds)
            : nil
        transportConfiguration.compression = HTTP2ClientTransport.Config.Compression(
            algorithm: .none,
            enabledAlgorithms: [.gzip]
        )

        if configuration.keepAliveTimeMilliseconds >= 0 {
            transportConfiguration.connection.keepalive = HTTP2ClientTransport.Config.Keepalive(
                time: .milliseconds(configuration.keepAliveTimeMilliseconds),
                timeout: .milliseconds(configuration.keepAliveTimeoutMilliseconds),
                allowWithoutCalls: configuration.keepAliveWithoutCalls
            )
        }

        let transport = try Transport(
            target: .dns(host: configuration.host, port: configuration.port),
            transportSecurity: configuration.plaintext ? .plaintext : .tls,
            config: transportConfiguration
        )
        let client = Client(transport: transport)

        self.client = client
        self.userAgent = configuration.userAgent
        self.connectionTask = Task {
            // Individual RPCs report connection errors. The connection task runs until shutdown.
            try? await client.runConnections()
        }
        super.init()
    }

    /// Stops accepting new calls and lets calls already in progress finish.
    @objc public func beginGracefulShutdown() {
        self.client.beginGracefulShutdown()
    }

    /// Abruptly stops the client and cancels calls in progress.
    @objc public func shutdownNow() {
        self.connectionTask.cancel()
    }

    /// Invokes `completion` after the connection task has stopped and all transport resources have
    /// been released. The callback can run on any Swift concurrency executor.
    @objc public func notifyWhenTerminated(_ completion: @escaping @Sendable () -> Void) {
        let connectionTask = self.connectionTask
        Task {
            await connectionTask.value
            completion()
        }
    }

    /// Starts one raw-message RPC.
    ///
    /// grpc-swift pulls request messages from `requestSource`. Kotlin pulls response events from
    /// the returned call. A negative timeout means that the call has no deadline.
    ///
    /// `waitForReady` controls whether a call waits for a connection after a transient failure.
    /// A negative `maxResponseMessageBytes` uses grpc-swift's default limit.
    ///
    /// For unary and server-streaming calls, `initialRequest` can carry the already validated
    /// request. The call then sends it without pulling from `requestSource`.
    @objc(
        startCallWithFullMethodName:type:headers:timeoutMilliseconds:waitForReady:maxResponseMessageBytes:
        compression:requestSource:initialRequest:error:
    )
    public func startCall(
        fullMethodName: String,
        type: SwiftGrpcMethodType,
        headers: SwiftGrpcMetadata,
        timeoutMilliseconds: Int64,
        waitForReady: SwiftGrpcWaitForReady,
        maxResponseMessageBytes: Int,
        compression: SwiftGrpcCompression,
        requestSource: any SwiftGrpcRequestSource,
        initialRequest: SwiftGrpcRequestBytes?
    ) throws -> SwiftGrpcCall {
        let descriptor = try MethodDescriptor(fullMethodName: fullMethodName, type: type)

        var options = CallOptions.defaults
        options.timeout = timeoutMilliseconds >= 0 ? .milliseconds(timeoutMilliseconds) : nil
        options.waitForReady = waitForReady.boolValue
        options.maxResponseMessageBytes = maxResponseMessageBytes >= 0 ? maxResponseMessageBytes : nil
        options.compression = compression.grpcCompression

        var metadata = headers.metadata
        if let userAgent = self.userAgent {
            metadata.replaceOrAddString(userAgent, forKey: "user-agent")
        }

        return SwiftGrpcCall(
            owner: self,
            descriptor: descriptor,
            options: options,
            metadata: metadata,
            requestSource: requestSource,
            initialRequest: initialRequest
        )
    }

    deinit {
        self.client.beginGracefulShutdown()
        self.connectionTask.cancel()
    }
}

extension MethodDescriptor {
    fileprivate init(fullMethodName: String, type: SwiftGrpcMethodType) throws {
        let components = fullMethodName.split(separator: "/", omittingEmptySubsequences: false)
        guard components.count == 2, components.allSatisfy({ !$0.isEmpty }) else {
            throw SwiftGrpcClientError.invalidFullMethodName(fullMethodName)
        }

        self.init(
            fullyQualifiedService: String(components[0]),
            method: String(components[1]),
            type: type.grpcType
        )
    }
}

extension SwiftGrpcMethodType {
    fileprivate var grpcType: MethodDescriptor.RPCType {
        switch self {
        case .unary:
            .unary
        case .clientStreaming:
            .clientStreaming
        case .serverStreaming:
            .serverStreaming
        case .bidirectionalStreaming:
            .bidirectionalStreaming
        }
    }
}

extension SwiftGrpcCompression {
    fileprivate var grpcCompression: CompressionAlgorithm {
        switch self {
        case .none:
            .none
        case .gzip:
            .gzip
        }
    }
}

private enum SwiftGrpcClientError: LocalizedError, Sendable {
    case invalidFullMethodName(String)

    var errorDescription: String? {
        switch self {
        case .invalidFullMethodName(let name):
            "Invalid gRPC method name '\(name)'. Use 'fully.qualified.Service/Method'."
        }
    }
}
