import _Concurrency

#if WendyNetBackendStandard && WendyNetBackendWendyLite
#error("WendyNet: only one of the 'Standard' or 'WendyLite' traits may be enabled.")
#elseif !WendyNetBackendStandard && !WendyNetBackendWendyLite
#error("WendyNet: enable exactly one of the 'Standard' or 'WendyLite' traits.")
#endif

let wendyNetMaximumMessageLength = 1024

// MARK: - ByteBuffer

/// Contiguous byte storage with read/write cursors suitable for zero-copy
/// networking. Avoids repeated allocation and copying associated with [UInt8].
///
/// Slicing returns a new ByteBuffer that shares backing storage via Swift's
/// built-in Array CoW.
public struct ByteBuffer: Sendable {
    private var _storage: [UInt8] = []
    private var _readerIndex: Int = 0
    private var _writerIndex: Int = 0

    public var readerIndex: Int { _readerIndex }
    public var writerIndex: Int { _writerIndex }
    public var readableBytes: Int { _writerIndex - _readerIndex }
    public var writableBytes: Int { capacity - _writerIndex }
    public var capacity: Int { _storage.count }

    public init() {}

    public init(bytes: [UInt8]) {
        _storage = bytes
        _writerIndex = bytes.count
    }

    // MARK: Slicing (shares storage, no copy)

    public func getSlice(at index: Int, length: Int) -> ByteBuffer? {
        guard index >= _readerIndex, index + length <= _writerIndex else { return nil }
        var slice = ByteBuffer()
        slice._storage = _storage
        slice._readerIndex = index
        slice._writerIndex = index + length
        return slice
    }

    public mutating func readSlice(length: Int) -> ByteBuffer? {
        guard readableBytes >= length else { return nil }
        let slice = getSlice(at: _readerIndex, length: length)
        _readerIndex += length
        return slice
    }

    public mutating func moveReaderIndex(by count: Int) {
        _readerIndex += count
    }

    // MARK: Copying reads (advance readerIndex)

    public mutating func readBytes(length: Int) -> [UInt8]? {
        guard readableBytes >= length else { return nil }
        let result = Array(_storage[_readerIndex ..< _readerIndex + length])
        _readerIndex += length
        return result
    }

    public mutating func readInteger<T: FixedWidthInteger>(as type: T.Type = T.self) -> T? {
        let size = MemoryLayout<T>.size
        guard readableBytes >= size else { return nil }
        var value: T = 0
        for i in 0 ..< size {
            value = (value << 8) | T(_storage[_readerIndex + i])
        }
        _readerIndex += size
        return value
    }

    // MARK: Peek (does not advance readerIndex)

    public func getInteger<T: FixedWidthInteger>(at index: Int, as type: T.Type = T.self) -> T? {
        let size = MemoryLayout<T>.size
        guard index >= _readerIndex, index + size <= _writerIndex else { return nil }
        var value: T = 0
        for i in 0 ..< size {
            value = (value << 8) | T(_storage[index + i])
        }
        return value
    }

    // MARK: Write (advances writerIndex)

    public mutating func writeBytes(_ bytes: [UInt8]) {
        _storage.append(contentsOf: bytes)
        _writerIndex += bytes.count
    }

    public mutating func writeBuffer(_ other: ByteBuffer) {
        writeBytes(Array(other._storage[other._readerIndex ..< other._writerIndex]))
    }

    public mutating func writeInteger<T: FixedWidthInteger>(_ value: T) {
        let size = MemoryLayout<T>.size
        for i in (0 ..< size).reversed() {
            _storage.append(UInt8(truncatingIfNeeded: value >> (i * 8)))
        }
        _writerIndex += size
    }

    public mutating func discardReadBytes() {
        guard _readerIndex > 0 else { return }
        _storage.removeFirst(_readerIndex)
        _writerIndex -= _readerIndex
        _readerIndex = 0
    }

    func readableBytesArray() -> [UInt8] {
        Array(_storage[_readerIndex ..< _writerIndex])
    }
}

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
public final class ComposedStage<A: PipelineStage, B: PipelineStage>: PipelineStage, @unchecked Sendable
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

// MARK: - Channel

/// A live network connection, generic on the message type produced by the pipeline.
///
/// Channel<ByteBuffer> = raw bytes, no pipeline.
/// Channel<ChatMessage> = pipeline decodes bytes into ChatMessages.
///
/// The pipeline is compiled into closures at build time. No dynamic casting.
/// The Channel only knows its final message type.
///
/// Backend-private: `ChannelCore<Message>` is defined by whichever backend file
/// the build includes (Backend+WendyLite.swift or Backend+Standard.swift). Both
/// expose the same internal surface this class consumes.
public final class Channel<Message: Sendable>: @unchecked Sendable {
    public let endpoint: Endpoint
    public let transport: TransportInfo

    /// Maximum message size the underlying transport can accept in a single write.
    public internal(set) var maximumMessageLength: Int

    /// Present after mTLS handshake with a Wendy peer.
    public internal(set) var remotePeerInfo: PeerInfo?

    let _core: ChannelCore<Message>?

    /// Receive the next inbound message. Suspends until available.
    /// Returns nil when the channel closes.
    public func receive() async throws(WendyNetError) -> Message? {
        guard let core = _core else {
            throw .connectionFailed
        }
        return try await core.receive()
    }

    /// Send a message through the pipeline. Handles backpressure internally.
    /// Returns .accepted if the message was enqueued for transmission,
    /// or .dropped if the transport discarded it (e.g. lossy UDP under congestion).
    @discardableResult
    public func send(_ msg: Message) async throws(WendyNetError) -> SendResult {
        guard let core = _core else {
            throw .connectionFailed
        }
        return try await core.send(msg)
    }

    public var isOpen: Bool { _core?.isOpen ?? false }

    public func close() async {
        await _core?.close()
    }

    init(
        endpoint: Endpoint,
        transport: TransportInfo,
        maximumMessageLength: Int,
        core: ChannelCore<Message>? = nil
    ) {
        self.endpoint = endpoint
        self.transport = transport
        self.maximumMessageLength = maximumMessageLength
        self._core = core
    }
}

// MARK: - Send Result

public enum SendResult: Sendable {
    /// The message was accepted and enqueued for transmission.
    case accepted

    /// The transport discarded the message (e.g. lossy UDP under congestion).
    case dropped
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
public final class Listener<Message: Sendable>: @unchecked Sendable {
    public let port: UInt16
    let core: ListenerCore<Message>?

    /// Accept the next inbound connection. Suspends until one arrives.
    /// Returns nil when the listener is closed.
    public func accept() async throws(WendyNetError) -> Channel<Message>? {
        guard let core else {
            throw .listenerError
        }
        return try await core.accept()
    }

    public func close() async {
        await core?.close()
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

// MARK: - Pipeline Closures

/// Closure bundle produced by a pipeline factory.
struct _PipelineClosures<Message>: Sendable {
    var decode: @Sendable (ByteBuffer, (Message) -> Void, (WendyNetError) -> Void) -> Void
    var encode: @Sendable (Message) -> ByteBuffer
    var started: @Sendable (ConnectionContext) -> Void
}

/// Closure bundle for a framer (ByteBuffer -> ByteBuffer, stream transports only).
struct _FramerClosures: Sendable {
    var decode: @Sendable (ByteBuffer, (ByteBuffer) -> Void, (WendyNetError) -> Void) -> Void
    var encode: @Sendable (ByteBuffer) -> ByteBuffer
    var started: @Sendable (ConnectionContext) -> Void

    /// Wrap a pipeline's closures so the framer runs first on inbound
    /// and last on outbound.
    func composing<Message>(_ inner: _PipelineClosures<Message>) -> _PipelineClosures<Message> {
        let framerDecode = self.decode
        let framerEncode = self.encode
        let framerStarted = self.started
        return _PipelineClosures<Message>(
            decode: { buf, emit, fail in
                framerDecode(buf, { frameBuf in
                    inner.decode(frameBuf, emit, fail)
                }, fail)
            },
            encode: { msg in
                framerEncode(inner.encode(msg))
            },
            started: { context in
                framerStarted(context)
                inner.started(context)
            }
        )
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
    var security: SecurityMode = .insecure
    var udpAssociationTimeoutSeconds: Int = 60
    let _pipelineFactory: @Sendable () -> _PipelineClosures<Message>
    var _framerFactory: (@Sendable () -> _FramerClosures)?

    /// Create a bootstrap with no pipeline. Produces Channel<ByteBuffer>.
    public init(wendyNet: WendyNet) where Message == ByteBuffer {
        self.wendyNet = wendyNet
        self._pipelineFactory = {
            _PipelineClosures(
                decode: { buf, emit, _ in emit(buf) },
                encode: { $0 },
                started: { _ in }
            )
        }
        self._framerFactory = nil
    }

    init(
        wendyNet: WendyNet,
        security: SecurityMode,
        udpTimeout: Int,
        pipelineFactory: @escaping @Sendable () -> _PipelineClosures<Message>,
        framerFactory: (@Sendable () -> _FramerClosures)?
    ) {
        self.wendyNet = wendyNet
        self.security = security
        self.udpAssociationTimeoutSeconds = udpTimeout
        self._pipelineFactory = pipelineFactory
        self._framerFactory = framerFactory
    }

    public func security(_ mode: SecurityMode) -> ClientBootstrap {
        var copy = self
        copy.security = mode
        return copy
    }

    public func udpAssociationTimeout(seconds: Int) -> ClientBootstrap {
        var copy = self
        copy.udpAssociationTimeoutSeconds = seconds
        return copy
    }

    /// Provide a framer factory. A new framer is created per connection and inserted
    /// only when the transport is stream-oriented. Datagram transports skip it.
    public func framer<F: PipelineStage & Sendable>(
        _ factory: @escaping @Sendable () -> F
    ) -> ClientBootstrap where F.Input == ByteBuffer, F.Output == ByteBuffer {
        var copy = self
        copy._framerFactory = {
            let framer = factory()
            return _FramerClosures(
                decode: { buf, emit, fail in framer.decode(buf, emit, fail) },
                encode: { buf in framer.encode(buf) },
                started: { context in framer.started(context: context) }
            )
        }
        return copy
    }

    /// Attach a pipeline. Each connection gets its own pipeline instances.
    /// Stages are listed sequentially; the compiler verifies types between them.
    public func pipeline<P: PipelineStage & Sendable>(
        @PipelineBuilder _ build: @escaping @Sendable () -> P
    ) -> ClientBootstrap<P.Output> where P.Input == ByteBuffer, P.Output: Sendable {
        let framer = _framerFactory
        return ClientBootstrap<P.Output>(
            wendyNet: wendyNet,
            security: security,
            udpTimeout: udpAssociationTimeoutSeconds,
            pipelineFactory: {
                let pipeline = build()
                return _PipelineClosures(
                    decode: { buf, emit, fail in pipeline.decode(buf, emit, fail) },
                    encode: { msg in pipeline.encode(msg) },
                    started: { context in pipeline.started(context: context) }
                )
            },
            framerFactory: framer
        )
    }
}

// MARK: - Server Bootstrap

/// Configures and binds a Listener that accepts inbound Channels.
///
/// `bind(port:)` lives in the active backend file.
public struct ServerBootstrap<Message: Sendable>: Sendable {
    let wendyNet: WendyNet
    var security: SecurityMode = .insecure
    var udpAssociationTimeoutSeconds: Int = 60
    let _pipelineFactory: @Sendable () -> _PipelineClosures<Message>
    var _framerFactory: (@Sendable () -> _FramerClosures)?

    public init(wendyNet: WendyNet) where Message == ByteBuffer {
        self.wendyNet = wendyNet
        self._pipelineFactory = {
            _PipelineClosures(
                decode: { buf, emit, _ in emit(buf) },
                encode: { $0 },
                started: { _ in }
            )
        }
        self._framerFactory = nil
    }

    init(
        wendyNet: WendyNet,
        security: SecurityMode,
        udpTimeout: Int,
        pipelineFactory: @escaping @Sendable () -> _PipelineClosures<Message>,
        framerFactory: (@Sendable () -> _FramerClosures)?
    ) {
        self.wendyNet = wendyNet
        self.security = security
        self.udpAssociationTimeoutSeconds = udpTimeout
        self._pipelineFactory = pipelineFactory
        self._framerFactory = framerFactory
    }

    public func security(_ mode: SecurityMode) -> ServerBootstrap {
        var copy = self
        copy.security = mode
        return copy
    }

    public func udpAssociationTimeout(seconds: Int) -> ServerBootstrap {
        var copy = self
        copy.udpAssociationTimeoutSeconds = seconds
        return copy
    }

    /// Provide a framer factory for accepted connections. Only inserted for stream transports.
    public func framer<F: PipelineStage & Sendable>(
        _ factory: @escaping @Sendable () -> F
    ) -> ServerBootstrap where F.Input == ByteBuffer, F.Output == ByteBuffer {
        var copy = self
        copy._framerFactory = {
            let framer = factory()
            return _FramerClosures(
                decode: { buf, emit, fail in framer.decode(buf, emit, fail) },
                encode: { buf in framer.encode(buf) },
                started: { context in framer.started(context: context) }
            )
        }
        return copy
    }

    /// Attach a pipeline for accepted connections.
    /// Stages are listed sequentially; the compiler verifies types between them.
    public func pipeline<P: PipelineStage & Sendable>(
        @PipelineBuilder _ build: @escaping @Sendable () -> P
    ) -> ServerBootstrap<P.Output> where P.Input == ByteBuffer, P.Output: Sendable {
        let framer = _framerFactory
        return ServerBootstrap<P.Output>(
            wendyNet: wendyNet,
            security: security,
            udpTimeout: udpAssociationTimeoutSeconds,
            pipelineFactory: {
                let pipeline = build()
                return _PipelineClosures(
                    decode: { buf, emit, fail in pipeline.decode(buf, emit, fail) },
                    encode: { msg in pipeline.encode(msg) },
                    started: { context in pipeline.started(context: context) }
                )
            },
            framerFactory: framer
        )
    }
}

// MARK: - WendyNet

/// Top-level entry point. Owns the device identity and discovery state.
public final class WendyNet: @unchecked Sendable {
    public var wendyCertificate: String?
    public var wendyPrivateKey: (any Key)?

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
    case tls(tls: TLSOptions)
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
