# kotlinx-rpc-grpc-swift

> [!WARNING]
> This package is intended only for use by the `kotlinx-rpc` library itself.
> **Its API is unstable and can change or break at any time, without notice or compatibility guarantees.**
> Applications must use the `kotlinx-rpc` APIs instead of depending on this package directly.

A Swift bridge between kotlinx-rpc and grpc-swift-2. The `GrpcSwiftBridge` library exposes
Objective-C APIs for calls, metadata, request buffers, and response events. It uses the
SwiftNIO Transport Services HTTP/2 transport with Apple's Network.framework.

## Requirements

- Swift tools 6.3 or newer with Xcode.
- iOS 18 or newer for client applications.
- macOS 15 or newer for package development and tests.

## Build and test

Run these commands from the repository root:

```sh
swift build
swift test
```

The test suite covers request-buffer access, metadata conversion, and response-event delivery,
cancellation, reentrant callbacks, and races. It doesn't require a running gRPC server.

To build for the iOS simulator:

```sh
xcodebuild -scheme kotlinx-rpc-grpc-swift -destination 'generic/platform=iOS Simulator' -configuration Release build
```

## Bridge contracts

- `SwiftGrpcClient` owns the connection task. Graceful shutdown lets active calls finish.
  Forceful shutdown cancels them. Termination callbacks run after the connection task stops.
- Request sources allow one outstanding pull. Each pull completes exactly once.
  Unary and server-streaming sources validate their request count before supplying a message.
- Response events arrive in `Headers? -> Message* -> Closed` order. The producer waits until
  the consumer takes each event. Only one response pull can be outstanding.
- Completion handlers can run synchronously or on a Swift concurrency executor.
  Callbacks run outside the response mailbox's lock.
- Non-OK gRPC statuses use closed events. Completion-handler errors report bridge failures.
- All writes to `SwiftGrpcRequestBytes must be finished before being sent. Access to request buffers
  and metadata must be sequential. Buffer pointers are valid only inside their callbacks.
