import _Concurrency

#if WendyNetBackendStandard && WendyNetBackendWendyLite
#error("WendyNet: only one of the 'Standard' or 'WendyLite' traits may be enabled.")
#elseif !WendyNetBackendStandard && !WendyNetBackendWendyLite
#error("WendyNet: enable exactly one of the 'Standard' or 'WendyLite' traits.")
#endif

// MARK: - ByteBuffer

#if WendyNetBackendStandard

@_exported import struct NIOCore.ByteBuffer

/// In Standard mode, WendyNet's `ByteBuffer` is SwiftNIO's `ByteBuffer`.
/// 
/// The WendyLite version contains a source-compatible subset of functionality.
public typealias ByteBuffer = NIOCore.ByteBuffer

#else

/// Contiguous byte storage with read/write cursors suitable for zero-copy
/// networking. Avoids repeated allocation and copying associated with [UInt8].
///
/// The method surface is a subset of SwiftNIO's `ByteBuffer` with matching
/// signatures, so pipelines compile against either backend; in Standard mode
/// this type is `typealias`'d to `NIOCore.ByteBuffer` directly.
///
/// Slicing returns a new ByteBuffer that shares backing storage via Swift's
/// built-in Array CoW.
public struct ByteBuffer: Sendable {
    private var storage: [UInt8] = []
    public private(set) var readerIndex: Int = 0
    public private(set) var writerIndex: Int = 0

    public var readableBytes: Int { writerIndex - readerIndex }
    public var writableBytes: Int { capacity - writerIndex }
    public var capacity: Int { storage.count }

    public init() {}

    public init(bytes: [UInt8]) {
        storage = bytes
        writerIndex = bytes.count
    }

    // MARK: Slicing (shares storage, no copy)

    public func getSlice(at index: Int, length: Int) -> ByteBuffer? {
        guard index >= readerIndex, index + length <= writerIndex else { return nil }
        var slice = ByteBuffer()
        slice.storage = storage
        slice.readerIndex = index
        slice.writerIndex = index + length
        return slice
    }

    public mutating func readSlice(length: Int) -> ByteBuffer? {
        guard readableBytes >= length else { return nil }
        let slice = getSlice(at: readerIndex, length: length)
        readerIndex += length
        return slice
    }

    public mutating func moveReaderIndex(forwardBy count: Int) {
        readerIndex += count
    }

    // MARK: Copying reads (advance readerIndex)

    public mutating func readBytes(length: Int) -> [UInt8]? {
        guard readableBytes >= length else { return nil }
        let result = Array(storage[readerIndex ..< readerIndex + length])
        readerIndex += length
        return result
    }

    public mutating func readInteger<T: FixedWidthInteger>(as type: T.Type = T.self) -> T? {
        let size = MemoryLayout<T>.size
        guard readableBytes >= size else { return nil }
        var value: T = 0
        for i in 0 ..< size {
            value = (value << 8) | T(storage[readerIndex + i])
        }
        readerIndex += size
        return value
    }

    // MARK: Peek (does not advance readerIndex)

    public func getInteger<T: FixedWidthInteger>(at index: Int, as type: T.Type = T.self) -> T? {
        let size = MemoryLayout<T>.size
        guard index >= readerIndex, index + size <= writerIndex else { return nil }
        var value: T = 0
        for i in 0 ..< size {
            value = (value << 8) | T(storage[index + i])
        }
        return value
    }

    /// Yields an `UnsafeRawBufferPointer` over the readable bytes. The pointer
    /// is only valid for the duration of `body`.
    public func withUnsafeReadableBytes<R, E: Error>(_ body: (UnsafeRawBufferPointer) throws(E) -> R) throws(E) -> R {
        var captured: Result<R, E>!
        storage.withUnsafeBytes { raw in
            let readable = UnsafeRawBufferPointer(rebasing: raw[readerIndex ..< writerIndex])
            do throws(E) {
                captured = .success(try body(readable))
            } catch {
                captured = .failure(error)
            }
        }
        return try captured.get()
    }

    // MARK: Write (advances writerIndex)

    /// Writes `bytes` at `writerIndex`, then advances `writerIndex` past them.
    private mutating func writeAtCursor(_ bytes: [UInt8]) {
        let end = writerIndex + bytes.count
        if writerIndex == storage.count {
            storage.append(contentsOf: bytes) // append at end (common case)
        } else if end <= storage.count {
            storage.replaceSubrange(writerIndex ..< end, with: bytes) // fully overwrite in range
        } else {
            let split = storage.count - writerIndex // overwrite the in-range part
            storage.replaceSubrange(writerIndex ..< storage.count, with: bytes[..<split])
            storage.append(contentsOf: bytes[split...]) // then extend with the rest
        }
        writerIndex = end
    }

    @discardableResult
    public mutating func writeBytes(_ bytes: [UInt8]) -> Int {
        writeAtCursor(bytes)
        return bytes.count
    }

    /// Consumes readable bytes from `buffer` and writes them at this buffer's
    /// write cursor.
    @discardableResult
    public mutating func writeBuffer(_ buffer: inout ByteBuffer) -> Int {
        let count = buffer.readableBytes
        guard count > 0 else { return 0 }
        writeAtCursor(Array(buffer.storage[buffer.readerIndex ..< buffer.writerIndex]))
        buffer.readerIndex += count
        return count
    }

    @discardableResult
    public mutating func writeInteger<T: FixedWidthInteger>(_ value: T) -> Int {
        let size = MemoryLayout<T>.size
        // Big-endian, written byte-by-byte at the cursor
        for shift in stride(from: (size - 1) * 8, through: 0, by: -8) {
            let byte = UInt8(truncatingIfNeeded: value >> shift)
            if writerIndex < storage.count {
                storage[writerIndex] = byte
            } else {
                storage.append(byte)
            }
            writerIndex += 1
        }
        return size
    }

    @discardableResult
    public mutating func discardReadBytes() -> Bool {
        guard readerIndex > 0 else { return false }
        storage.removeFirst(readerIndex)
        writerIndex -= readerIndex
        readerIndex = 0
        return true
    }
}

#endif

// MARK: - Connection Context

/// Metadata about the resolved connection, available to pipeline stages.
public struct ConnectionContext: Sendable {
    public var remoteEndpoint: Endpoint
    public var transport: TransportInfo
    public var security: SecurityMode

    public init(remoteEndpoint: Endpoint, transport: TransportInfo, security: SecurityMode) {
        self.remoteEndpoint = remoteEndpoint
        self.transport = transport
        self.security = security
    }
}

// MARK: - Pipeline Stage

/// A typed processing step in a message pipeline. Each stage transforms
/// Input to Output (inbound/decode) and Output back to Input (outbound/encode).
///
/// Stages are composed at compile time via .then(). The compiler verifies
/// that each stage's Output matches the next stage's Input. The fully composed
/// pipeline is captured as closures, avoiding existentials.
///
/// decode() may emit zero or more outputs per input (e.g. a framer buffers
/// partial data and emits complete messages).
public protocol PipelineStage: AnyObject {
    associatedtype Input
    associatedtype Output

    /// Called once after the connection is established, before any data flows.
    /// Provides endpoint, transport and security metadata. Default is a no-op.
    func started(context: ConnectionContext)

    /// Inbound: process one input, emit zero or more outputs.
    /// Call fail() to signal a protocol error. This propagates to Channel.receive().
    func decode(_ input: Input, _ emit: (Output) -> Void, _ fail: (WendyNetError) -> Void)

    /// Outbound: encode one output back to input for the layer below.
    func encode(_ output: Output) -> Input
}

extension PipelineStage {
    public func started(context: ConnectionContext) {}
}

/// Chains two pipeline stages at compile time.
/// A.Output must equal B.Input. Enforced by the generic constraint.
public final class ComposedStage<A: PipelineStage, B: PipelineStage>: PipelineStage
    where A.Output == B.Input
{
    public typealias Input = A.Input
    public typealias Output = B.Output

    private let first: A
    private let second: B

    public init(_ first: A, _ second: B) {
        self.first = first
        self.second = second
    }

    public func started(context: ConnectionContext) {
        first.started(context: context)
        second.started(context: context)
    }

    public func decode(_ input: A.Input, _ emit: (B.Output) -> Void, _ fail: (WendyNetError) -> Void) {
        first.decode(input, { intermediate in
            self.second.decode(intermediate, emit, fail)
        }, fail)
    }

    public func encode(_ output: B.Output) -> A.Input {
        first.encode(second.encode(output))
    }
}

extension PipelineStage {
    /// Chain this stage with another, producing a composed stage.
    /// The compiler verifies Self.Output == Next.Input.
    public func then<Next: PipelineStage>(_ next: Next) -> ComposedStage<Self, Next>
        where Self.Output == Next.Input
    {
        ComposedStage(self, next)
    }
}

// MARK: - Pipeline Builder

@resultBuilder
public struct PipelineBuilder {
    public static func buildPartialBlock<S: PipelineStage>(first: S) -> S {
        first
    }

    public static func buildPartialBlock<A: PipelineStage, B: PipelineStage>(
        accumulated: A, next: B
    ) -> ComposedStage<A, B> where A.Output == B.Input {
        accumulated.then(next)
    }
}

// MARK: - Inbound / Outbound

/// Iterator over decoded messages arriving on a channel.
///
/// Iterate with `while let msg = try await inbound.next() { ... }`. Returns nil
/// once the channel closes cleanly. Throws `.cancelled` if the surrounding Task
/// is cancelled while suspended. Decoded messages already buffered are delivered
/// even after cancellation.
///
/// This is a unicast iterator: only one consumer at a time. Invoking `next()`
/// from a concurrent context that contends with another such call throws
/// `WendyNetError.concurrentAccess`.
public struct Inbound<Message: Sendable>: Sendable {
    let nextStep: @Sendable () async -> InboundStep<Message>

    public func next() async throws(WendyNetError) -> Message? {
        switch await nextStep() {
        case .message(let m): return m
        case .end: return nil
        case .failure(let error): throw error
        }
    }
}

/// Sink for sending messages on a channel.
///
/// `write` honours cancellation: if the surrounding Task is cancelled while the
/// send is suspended (waiting for backpressure relief or transport completion)
/// it throws `.cancelled`.
public struct Outbound<Message: Sendable>: Sendable {
    let writeStep: @Sendable (Message) async -> OutboundStep

    public func write(_ msg: Message) async throws(WendyNetError) {
        switch await writeStep(msg) {
        case .accepted: return
        case .failure(let error): throw error
        }
    }
}

/// Iterator over connections accepted by a listener.
///
/// Iterate with `while let channel = try await accepted.next() { ... }`. Returns
/// nil once the listener closes. Throws `.cancelled` if the surrounding Task is
/// cancelled while suspended.
///
/// This is a unicast iterator: only one consumer at a time. Invoking `next()`
/// from a concurrent context that contends with another such call throws
/// `WendyNetError.concurrentAccess`.
public struct Accepted<Message: Sendable>: Sendable {
    let nextStep: @Sendable () async -> AcceptedStep<Message>

    public func next() async throws(WendyNetError) -> Channel<Message>? {
        switch await nextStep() {
        case .channel(let c): return c
        case .end: return nil
        case .failure(let error): throw error
        }
    }
}

// MARK: - Channel

/// A live network connection, generic on the message type produced by the pipeline.
///
/// Channel<ByteBuffer> = raw bytes, no pipeline.
/// Channel<ChatMessage> = pipeline decodes bytes into ChatMessages.
///
/// The pipeline is compiled into closures at build time. No dynamic casting.
/// The Channel only knows its final message type.
///
/// ## Lifetime
///
/// A Channel is driven inside a structured scope via `executeThenClose`. The
/// channel is automatically closed when the body returns or throws. There is
/// no separate `close()` method by design -- this guarantees that resources are
/// released exactly once, on a known boundary, even under cancellation.
///
/// ```swift
/// try await channel.executeThenClose { inbound, outbound in
///     try await outbound.write(message)
///     while let reply = try await inbound.next() {
///         // ...
///     }
/// }
/// ```
public final class Channel<Message: Sendable>: Sendable {
    public let endpoint: Endpoint
    public let transport: TransportInfo

    /// Maximum message size the underlying transport can accept in a single write.
    public let maximumMessageLength: Int

    /// Present after mTLS handshake with a Wendy peer.
    public let remotePeerInfo: PeerInfo?

    let core: ChannelCore<Message>?

    /// Drive the channel inside a structured scope.
    ///
    /// `body` receives an `Inbound` to read decoded messages and an `Outbound`
    /// to send them. The channel is closed when `body` returns or throws,
    /// including via cancellation propagation.
    ///
    /// If the surrounding Task is cancelled while a `next()` or `write(_:)` call
    /// is suspended, that call throws `.cancelled`.
    public func executeThenClose<R: Sendable>(
        _ body: @Sendable (Inbound<Message>, Outbound<Message>) async throws(WendyNetError) -> R
    ) async throws(WendyNetError) -> R {
        guard let core = core else {
            throw .connectionFailed
        }
        return try await core.executeThenClose(body)
    }

    init(
        endpoint: Endpoint,
        transport: TransportInfo,
        maximumMessageLength: Int,
        remotePeerInfo: PeerInfo? = nil,
        core: ChannelCore<Message>? = nil
    ) {
        self.endpoint = endpoint
        self.transport = transport
        self.maximumMessageLength = maximumMessageLength
        self.remotePeerInfo = remotePeerInfo
        self.core = core
    }
}

// MARK: - Transport Info

public struct TransportInfo: Sendable {
    public var kind: TransportKind
    public var isStream: Bool
    public var isDatagramOriented: Bool { !isStream }

    public init(kind: TransportKind, isStream: Bool) {
        self.kind = kind
        self.isStream = isStream
    }
}

public enum TransportKind: Sendable {
    case tcp
    case udp
    case bluetoothL2CAP
}

// MARK: - Peer Info

public struct PeerInfo: Sendable {
    public var publicKey: String
    public var customerId: String?
    public var fleetIds: [String]?

    public init(publicKey: String, customerId: String? = nil, fleetIds: [String]? = nil) {
        self.publicKey = publicKey
        self.customerId = customerId
        self.fleetIds = fleetIds
    }
}

// MARK: - Listener

/// A bound server that accepts inbound connections.
///
/// ## Lifetime
///
/// A Listener is driven inside a structured scope via `executeThenClose`. The
/// listener is automatically closed when the body returns or throws. Accepted
/// channels are exposed via the `Accepted` iterator; each accepted channel
/// must itself be driven via its own `executeThenClose`.
///
/// Embedded Swift forbids existential `any Error`, so `withThrowingTaskGroup`
/// is unavailable. Use a non-throwing `withTaskGroup` and contain the accept
/// loop's typed throws inside a local `do`:
///
/// ```swift
/// try await listener.executeThenClose { (accepted: Accepted<Message>) throws(WendyNetError) -> Void in
///     await withTaskGroup(of: Void.self) { group in
///         do throws(WendyNetError) {
///             while let channel = try await accepted.next() {
///                 group.addTask {
///                     _ = try? await channel.executeThenClose { inbound, outbound in
///                         // handle one connection
///                     }
///                 }
///             }
///         } catch {
///             // .closed or .cancelled -- fall through and tear the group down.
///         }
///         group.cancelAll()
///     }
/// }
/// ```
public final class Listener<Message: Sendable>: Sendable {
    public let port: UInt16
    let core: ListenerCore<Message>?

    /// Drive the listener inside a structured scope.
    ///
    /// `body` receives an `Accepted` iterator over incoming connections. The
    /// listener is closed when `body` returns or throws.
    ///
    /// If the surrounding Task is cancelled while `accepted.next()` is suspended,
    /// it throws `.cancelled`.
    public func executeThenClose<R: Sendable>(
        _ body: @Sendable (Accepted<Message>) async throws(WendyNetError) -> R
    ) async throws(WendyNetError) -> R {
        guard let core else {
            throw .listenerError
        }
        return try await core.executeThenClose(body)
    }

    init(port: UInt16) {
        self.port = port
        self.core = nil
    }

    init(port: UInt16, core: ListenerCore<Message>) {
        self.port = port
        self.core = core
    }
}

// MARK: - Client Bootstrap

/// Configures and creates outbound Channels.
///
/// The generic parameter Message is determined by the pipeline. Without a
/// pipeline, Message defaults to ByteBuffer (raw bytes). Call .pipeline()
/// to get a bootstrap that produces typed channels.
///
/// An optional .framer() provides a ByteBuffer -> ByteBuffer stage that is
/// only inserted when the selected transport is stream-oriented (TCP, L2CAP).
/// Datagram transports (UDP) skip it because they already have message boundaries.
///
/// `connect(to:)` lives in the active backend file.
public struct ClientBootstrap<Message: Sendable>: Sendable {
    let wendyNet: WendyNet
    var config: BootstrapConfig<Message>

    // Accessors used by the backend `connect(to:)` extensions.
    var security: SecurityMode { config.security }
    var udpAssociationTimeoutSeconds: Int { config.udpAssociationTimeoutSeconds }
    var pipelineFactory: @Sendable () -> PipelineClosures<Message> { config.pipelineFactory }
    var framerFactory: (@Sendable () -> FramerClosures)? { config.framerFactory }

    /// Create a bootstrap with no pipeline. Produces Channel<ByteBuffer>.
    public init(wendyNet: WendyNet) where Message == ByteBuffer {
        self.wendyNet = wendyNet
        self.config = .passthrough()
    }

    init(wendyNet: WendyNet, config: BootstrapConfig<Message>) {
        self.wendyNet = wendyNet
        self.config = config
    }

    public func security(_ mode: SecurityMode) -> ClientBootstrap {
        var copy = self
        copy.config.security = mode
        return copy
    }

    public func udpAssociationTimeout(seconds: Int) -> ClientBootstrap {
        var copy = self
        copy.config.udpAssociationTimeoutSeconds = seconds
        return copy
    }

    /// Provide a framer factory. A new framer is created per connection and inserted
    /// only when the transport is stream-oriented. Datagram transports skip it.
    public func framer<F: PipelineStage & SendableMetatype>(
        _ factory: @escaping @Sendable () -> F
    ) -> ClientBootstrap where F.Input == ByteBuffer, F.Output == ByteBuffer {
        var copy = self
        copy.config.framerFactory = makeFramerClosures(factory)
        return copy
    }

    /// Attach a pipeline. Each connection gets its own pipeline instances.
    /// Stages are listed sequentially; the compiler verifies types between them.
    public func pipeline<P: PipelineStage & SendableMetatype>(
        @PipelineBuilder _ build: @escaping @Sendable () -> P
    ) -> ClientBootstrap<P.Output> where P.Input == ByteBuffer, P.Output: Sendable {
        ClientBootstrap<P.Output>(
            wendyNet: wendyNet,
            config: config.reframed(pipelineFactory: makePipelineClosures(build))
        )
    }
}

// MARK: - Server Bootstrap

/// Configures and binds a Listener that accepts inbound Channels.
///
/// `bind(port:)` lives in the active backend file.
public struct ServerBootstrap<Message: Sendable>: Sendable {
    let wendyNet: WendyNet
    var config: BootstrapConfig<Message>

    // Accessors used by the backend `bind(port:)` extensions.
    var security: SecurityMode { config.security }
    var udpAssociationTimeoutSeconds: Int { config.udpAssociationTimeoutSeconds }
    var pipelineFactory: @Sendable () -> PipelineClosures<Message> { config.pipelineFactory }
    var framerFactory: (@Sendable () -> FramerClosures)? { config.framerFactory }

    public init(wendyNet: WendyNet) where Message == ByteBuffer {
        self.wendyNet = wendyNet
        self.config = .passthrough()
    }

    init(wendyNet: WendyNet, config: BootstrapConfig<Message>) {
        self.wendyNet = wendyNet
        self.config = config
    }

    public func security(_ mode: SecurityMode) -> ServerBootstrap {
        var copy = self
        copy.config.security = mode
        return copy
    }

    public func udpAssociationTimeout(seconds: Int) -> ServerBootstrap {
        var copy = self
        copy.config.udpAssociationTimeoutSeconds = seconds
        return copy
    }

    /// Provide a framer factory for accepted connections. Only inserted for stream transports.
    public func framer<F: PipelineStage & SendableMetatype>(
        _ factory: @escaping @Sendable () -> F
    ) -> ServerBootstrap where F.Input == ByteBuffer, F.Output == ByteBuffer {
        var copy = self
        copy.config.framerFactory = makeFramerClosures(factory)
        return copy
    }

    /// Attach a pipeline for accepted connections.
    /// Stages are listed sequentially; the compiler verifies types between them.
    public func pipeline<P: PipelineStage & SendableMetatype>(
        @PipelineBuilder _ build: @escaping @Sendable () -> P
    ) -> ServerBootstrap<P.Output> where P.Input == ByteBuffer, P.Output: Sendable {
        ServerBootstrap<P.Output>(
            wendyNet: wendyNet,
            config: config.reframed(pipelineFactory: makePipelineClosures(build))
        )
    }
}

// MARK: - WendyNet

/// Top-level entry point. Owns discovery state.
///
/// Currently stateless; reserved as the identity/discovery surface for the
/// not-yet-implemented mDNS/BLE peer discovery story.
public final class WendyNet: Sendable {
    public init() {}

    /// Start discovering peers. Cancel the consuming Task to stop.
    public func discover(options: DiscoveryOptions = DiscoveryOptions()) -> DiscoveryStream {
        let _ = options
        fatalError("not yet implemented")
    }

    public func startAdvertising(options: AdvertisingOptions = AdvertisingOptions()) async throws(WendyNetError) {
        let _ = options
    }

    public func stopAdvertising() async {}
}

// MARK: - Discovery

public enum DiscoveryEvent: Sendable {
    case peerDiscovered(DiscoveredPeer)
    case peerLost(DiscoveredPeer)
}

public struct DiscoveryStream: Sendable {
    public init() {}

    public func next() async -> DiscoveryEvent? {
        fatalError("not yet implemented")
    }
}

// MARK: - Errors

public enum WendyNetError: Error, Sendable {
    case discoveryError
    case listenerError
    case connectionFailed
    case pipelineError
    case protocolError
    case closed
    /// `executeThenClose` was called more than once on the same `Channel` or
    /// `Listener`. The resource was consumed and closed by the prior call.
    case alreadyConsumed
    /// `next()` was invoked from a concurrent context that contends with
    /// another such call. `Inbound` and `Accepted` are unicast: only one
    /// consumer may iterate at a time. See the docs on those types.
    case concurrentAccess
    /// The surrounding Task was cancelled while the operation was suspended.
    ///
    /// This is the typed-throws analogue of `CancellationError` -- we cannot use
    /// `CancellationError` directly because every WendyNet API uses typed throws
    /// with `WendyNetError` for Embedded Swift compatibility.
    case cancelled
}

// MARK: - Endpoint

public enum Endpoint: Sendable {
    case wendyPeer(publicKey: String, port: UInt16)
    case ipHost(hostname: String, port: UInt16)

    public init(wendyPeerPublicKey publicKey: String, port: UInt16) {
        self = .wendyPeer(publicKey: publicKey, port: port)
    }

    public init(hostname: String, port: UInt16) {
        self = .ipHost(hostname: hostname, port: port)
    }
}

// MARK: - Discovered Peer

public struct DiscoveredPeer: Sendable {
    public var publicKey: String
    public var customerId: String
    public var fleetIds: [String]

    public init(publicKey: String, customerId: String, fleetIds: [String]) {
        self.publicKey = publicKey
        self.customerId = customerId
        self.fleetIds = fleetIds
    }

    public func endpoint(port: UInt16) -> Endpoint {
        .wendyPeer(publicKey: publicKey, port: port)
    }
}

// MARK: - Options

public struct AdvertisingOptions: Sendable {
    public var bluetoothEnabled = true
    public var mdnsEnabled = true

    public init() {}
}

public struct DiscoveryOptions: Sendable {
    public var bluetoothEnabled = true
    public var mdnsEnabled = true
    public var customerIdFilter: String?
    public var fleetIdFilter: [String] = []

    public init() {}
}

// MARK: - Security

public enum SecurityMode: Sendable {
    case wendyPeer
    case insecure
    case tls(TLSOptions)
}

public struct TLSOptions: Sendable {
    public var trustedRoots: CertificateRoots = .custom([])
    public var authenticateRemote: Bool = false
    public var localCertificate: String? = nil
    public var localPrivateKey: (any Key)? = nil

    public init() {}
}

public enum CertificateRoots: Sendable {
    case system
    case webPKI
    case custom([String])
}

public protocol Key: Sendable {
    func sign(data: [UInt8]) -> [UInt8]
    func verify(signature: [UInt8], data: [UInt8]) -> Bool
}
