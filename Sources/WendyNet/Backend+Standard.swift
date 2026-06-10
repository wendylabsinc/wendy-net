#if WendyNetBackendStandard
import NIOCore
import NIOPosix
import NIOConcurrencyHelpers
import _Concurrency

// MARK: - Internal lock primitive
//
// Standard-backend variant of the lock-box pattern. The canonical rationale
// lives in `wendy-lite/Sources/WendyLite/Internal.swift`; this version uses
// `NIOLock` instead of an atomic spinlock because the Standard backend runs
// on host platforms where NIO's lock is already available and is the right
// choice. Like its WendyLite-backend sibling, it accepts non-`Sendable` `T`
// so it can wrap user pipeline-stage closures (mirroring SwiftNIO's
// `ChannelHandler` confinement), and is `@unchecked Sendable` because the
// compiler can't prove the lock discipline itself.
//
// `NIOLockedValueBox` is only `@unchecked Sendable where Value: Sendable`
// and would not accept our `State` containing non-`Sendable` user closures,
// which is why we need this carrier rather than NIO's stock primitive.

fileprivate final class LockedBox<T>: @unchecked Sendable {
    private let lock = NIOLock()
    private var value: T

    init(_ value: T) {
        self.value = value
    }

    func withLockedValue<R>(_ body: (inout T) throws -> R) rethrows -> R {
        lock.lock()
        defer { lock.unlock() }
        return try body(&value)
    }
}

private let standardEventLoopGroup = MultiThreadedEventLoopGroup.singleton

fileprivate typealias NIOByteBufferChannel = NIOAsyncChannel<ByteBuffer, ByteBuffer>
fileprivate typealias NIOServerInboundChannel = NIOAsyncChannel<NIOByteBufferChannel, Never>

// MARK: - Inbound / outbound bridges

/// Pulls `ByteBuffer`s from a NIO async-channel inbound stream, enforcing the
/// single-consumer contract: a second concurrent pull observing the claimed-out
/// iterator surfaces `.concurrentAccess`.
private final class NIOByteSource: Sendable {
    private struct State {
        var iterator: NIOAsyncChannelInboundStream<ByteBuffer>.AsyncIterator?
    }
    private let state: LockedBox<State>

    init(iterator: NIOAsyncChannelInboundStream<ByteBuffer>.AsyncIterator) {
        self.state = LockedBox(State(iterator: iterator))
    }

    func pull() async -> Result<ByteBuffer?, WendyNetError> {
        let claimed: NIOAsyncChannelInboundStream<ByteBuffer>.AsyncIterator? =
            state.withLockedValue { s in
                let i = s.iterator
                s.iterator = nil
                return i
            }
        guard var iter = claimed else {
            return .failure(.concurrentAccess)
        }
        let result: Result<ByteBuffer?, WendyNetError>
        do {
            result = .success(try await iter.next())
        } catch is CancellationError {
            result = .failure(.cancelled)
        } catch {
            result = .failure(.connectionFailed)
        }
        state.withLockedValue { s in s.iterator = iter }
        return result
    }
}

private final class InboundBridge<Message: Sendable>: Sendable {
    private struct State {
        var pending: [Message] = []
        var error: WendyNetError? = nil
        var ended = false
        var decode: (ByteBuffer, (Message) -> Void, (WendyNetError) -> Void) -> Void
    }
    private let state: LockedBox<State>
    private let pull: @Sendable () async -> Result<ByteBuffer?, WendyNetError>

    init(
        decode: @escaping (ByteBuffer, (Message) -> Void, (WendyNetError) -> Void) -> Void,
        pull: @escaping @Sendable () async -> Result<ByteBuffer?, WendyNetError>
    ) {
        self.state = LockedBox(State(decode: decode))
        self.pull = pull
    }

    func next() async -> InboundStep<Message> {
        while true {
            // Fast path: buffered message or terminal state.
            let fast: InboundStep<Message>? = state.withLockedValue { s in
                if !s.pending.isEmpty { return .message(s.pending.removeFirst()) }
                if let err = s.error { return .failure(err) }
                if s.ended { return .end }
                return nil
            }
            if let fast { return fast }

            let pulled = await pull()

            let result: InboundStep<Message>? = state.withLockedValue { s in
                switch pulled {
                case .success(let bufOpt):
                    guard let buf = bufOpt else {
                        s.ended = true
                        return .end
                    }
                    // Zero-copy hand-off: pipeline sees the source buffer directly.
                    s.decode(buf, { msg in
                        s.pending.append(msg)
                    }, { err in
                        s.error = err
                    })
                    if let err = s.error { return .failure(err) }
                    if !s.pending.isEmpty { return .message(s.pending.removeFirst()) }
                    return nil  // decoded zero messages; pull again
                case .failure(let err):
                    s.ended = true
                    return .failure(err)
                }
            }
            if let result { return result }
        }
    }
}

private final class OutboundBridge<Message: Sendable>: Sendable {
    private struct State {
        var encode: (Message) -> ByteBuffer
    }
    private let state: LockedBox<State>
    private let sink: @Sendable (ByteBuffer) async -> OutboundStep

    init(
        encode: @escaping (Message) -> ByteBuffer,
        sink: @escaping @Sendable (ByteBuffer) async -> OutboundStep
    ) {
        self.state = LockedBox(State(encode: encode))
        self.sink = sink
    }

    func write(_ msg: Message) async -> OutboundStep {
        // Encode under lock (may mutate user stage state); the CoW buffer then
        // travels to the sink without copying.
        let buf = state.withLockedValue { s in s.encode(msg) }
        return await sink(buf)
    }
}

// MARK: - Accept bridge

private final class AcceptIteratorBox<Message: Sendable>: Sendable {
    private struct State {
        // `iterator` is briefly nil while a (single) caller is awaiting the
        // next accepted connection outside the lock; a second concurrent call
        // seeing nil here surfaces `.concurrentAccess` to the caller.
        var iterator: NIOAsyncChannelInboundStream<NIOByteBufferChannel>.AsyncIterator?
        var ended = false
        var error: WendyNetError? = nil
    }
    private let state: LockedBox<State>
    private let context: ConnectionContext
    private let pipelineFactory: @Sendable () -> PipelineClosures<Message>
    private let framerFactory: (@Sendable () -> FramerClosures)?

    init(
        iterator: NIOAsyncChannelInboundStream<NIOByteBufferChannel>.AsyncIterator,
        context: ConnectionContext,
        pipelineFactory: @escaping @Sendable () -> PipelineClosures<Message>,
        framerFactory: (@Sendable () -> FramerClosures)?
    ) {
        self.state = LockedBox(State(iterator: iterator))
        self.context = context
        self.pipelineFactory = pipelineFactory
        self.framerFactory = framerFactory
    }

    func next() async -> AcceptedStep<Message> {
        // Fast path: terminal state.
        let fast: AcceptedStep<Message>? = state.withLockedValue { s in
            if let err = s.error { return .failure(err) }
            if s.ended { return .end }
            return nil
        }
        if let fast { return fast }

        // Claim the iterator. nil here means another caller is already awaiting;
        // surface as `.concurrentAccess`.
        let claimed: NIOAsyncChannelInboundStream<NIOByteBufferChannel>.AsyncIterator? = state.withLockedValue { s in
            let i = s.iterator
            s.iterator = nil
            return i
        }
        guard var iter = claimed else {
            return .failure(.concurrentAccess)
        }

        let pull: Result<NIOByteBufferChannel?, Error>
        do {
            let childNIO = try await iter.next()
            pull = .success(childNIO)
        } catch {
            pull = .failure(error)
        }

        return state.withLockedValue { s in
            s.iterator = iter
            switch pull {
            case .success(let opt):
                guard let childNIO = opt else {
                    s.ended = true
                    return .end
                }
                return makeAcceptedChannel(childNIO: childNIO)
            case .failure(let err):
                s.ended = true
                if err is CancellationError {
                    return .failure(.cancelled)
                }
                return .failure(.listenerError)
            }
        }
    }

    private func makeAcceptedChannel(childNIO: NIOByteBufferChannel) -> AcceptedStep<Message> {
        var closures = pipelineFactory()
        if let framer = framerFactory {
            closures = framer().composing(closures)
        }
        closures.started(context)

        let core = ChannelCore<Message>(
            nioAsyncChannel: childNIO,
            endpoint: context.remoteEndpoint,
            transport: context.transport,
            decode: closures.decode,
            encode: closures.encode
        )
        let channel = Channel<Message>(
            endpoint: context.remoteEndpoint,
            transport: context.transport,
            maximumMessageLength: wendyNetMaximumMessageLength,
            core: core
        )
        return .channel(channel)
    }
}

// MARK: - ChannelCore
//
// Sendable. The non-Sendable pipeline closures live inside `LockedBox<Closures?>`
// and are consumed (set to nil) on the first call to `executeThenClose`,
// after which the bridges own them. A second call sees nil and fatal-errors:
// `executeThenClose` is documented as one-shot, and two concurrent calls
// would race on the user's pipeline-stage state through separately-locked
// bridges. The atomic check-and-clear inside `withLockedValue` makes that
// race impossible -- exactly one caller wins.

final class ChannelCore<Message: Sendable>: Sendable {
    let endpoint: Endpoint
    let transport: TransportInfo
    fileprivate let nioAsyncChannel: NIOByteBufferChannel
    private struct Closures {
        let decode: (ByteBuffer, (Message) -> Void, (WendyNetError) -> Void) -> Void
        let encode: (Message) -> ByteBuffer
    }
    private let closures: LockedBox<Closures?>

    fileprivate init(
        nioAsyncChannel: NIOByteBufferChannel,
        endpoint: Endpoint,
        transport: TransportInfo,
        decode: @escaping (ByteBuffer, (Message) -> Void, (WendyNetError) -> Void) -> Void,
        encode: @escaping (Message) -> ByteBuffer
    ) {
        self.nioAsyncChannel = nioAsyncChannel
        self.endpoint = endpoint
        self.transport = transport
        self.closures = LockedBox(Closures(decode: decode, encode: encode))
    }

    func executeThenClose<R: Sendable>(
        _ body: @Sendable (Inbound<Message>, Outbound<Message>) async throws(WendyNetError) -> R
    ) async throws(WendyNetError) -> R {
        // Take ownership of the closures by setting the slot to nil. Atomic
        // with respect to a concurrent second call -- exactly one caller sees
        // the closures, the other gets `.alreadyConsumed`.
        let taken: Closures? = closures.withLockedValue { c in
            let v = c
            c = nil
            return v
        }
        guard let taken else {
            throw .alreadyConsumed
        }
        let decodeClosure = taken.decode
        let encodeClosure = taken.encode

        let outcome: Result<R, WendyNetError>
        do {
            outcome = try await nioAsyncChannel.executeThenClose { nioInbound, nioOutbound -> Result<R, WendyNetError> in
                let source = NIOByteSource(iterator: nioInbound.makeAsyncIterator())
                let inboundBridge = InboundBridge<Message>(
                    decode: decodeClosure,
                    pull: { [source] in await source.pull() }
                )
                let outboundBridge = OutboundBridge<Message>(
                    encode: encodeClosure,
                    sink: { [nioOutbound] buf in
                        do {
                            try await nioOutbound.write(buf)
                        } catch is CancellationError {
                            return .failure(.cancelled)
                        } catch {
                            return .failure(.connectionFailed)
                        }
                        return .accepted
                    }
                )

                let inbound = Inbound<Message>(nextStep: { [inboundBridge] in
                    await inboundBridge.next()
                })
                let outbound = Outbound<Message>(writeStep: { [outboundBridge] msg in
                    await outboundBridge.write(msg)
                })

                do throws(WendyNetError) {
                    let v = try await body(inbound, outbound)
                    return Result<R, WendyNetError>.success(v)
                } catch {
                    return Result<R, WendyNetError>.failure(error)
                }
            }
        } catch is CancellationError {
            throw .cancelled
        } catch {
            throw .connectionFailed
        }
        switch outcome {
        case .success(let v): return v
        case .failure(let e): throw e
        }
    }
}

// MARK: - ListenerCore


final class ListenerCore<Message: Sendable>: Sendable {
    let port: UInt16
    let context: ConnectionContext
    fileprivate let serverChannel: NIOServerInboundChannel
    private let pipelineFactory: @Sendable () -> PipelineClosures<Message>
    private let framerFactory: (@Sendable () -> FramerClosures)?
    /// Single-use latch -- see `executeThenClose` for rationale.
    private let used = LockedBox<Bool>(false)

    fileprivate init(
        port: UInt16,
        context: ConnectionContext,
        serverChannel: NIOServerInboundChannel,
        pipelineFactory: @escaping @Sendable () -> PipelineClosures<Message>,
        framerFactory: (@Sendable () -> FramerClosures)?
    ) {
        self.port = port
        self.context = context
        self.serverChannel = serverChannel
        self.pipelineFactory = pipelineFactory
        self.framerFactory = framerFactory
    }

    func executeThenClose<R: Sendable>(
        _ body: @Sendable (Accepted<Message>) async throws(WendyNetError) -> R
    ) async throws(WendyNetError) -> R {
        // Atomic check-and-set: exactly one caller wins. A second concurrent
        // or sequential call fatal-errors. `executeThenClose` is one-shot
        // by design -- the underlying NIO accept iterator can only be created
        // once and the server channel is closed on exit.
        let wasAlreadyUsed: Bool = used.withLockedValue { u in
            let prev = u
            u = true
            return prev
        }
        if wasAlreadyUsed {
            throw .alreadyConsumed
        }

        let context = self.context
        let pipelineFactory = self.pipelineFactory
        let framerFactory = self.framerFactory

        let outcome: Result<R, WendyNetError>
        do {
            outcome = try await serverChannel.executeThenClose { acceptStream -> Result<R, WendyNetError> in
                let bridge = AcceptIteratorBox<Message>(
                    iterator: acceptStream.makeAsyncIterator(),
                    context: context,
                    pipelineFactory: pipelineFactory,
                    framerFactory: framerFactory
                )
                let accepted = Accepted<Message>(nextStep: { [bridge] in
                    await bridge.next()
                })

                do throws(WendyNetError) {
                    let v = try await body(accepted)
                    return Result<R, WendyNetError>.success(v)
                } catch {
                    return Result<R, WendyNetError>.failure(error)
                }
            }
        } catch is CancellationError {
            throw .cancelled
        } catch {
            throw .listenerError
        }
        switch outcome {
        case .success(let v): return v
        case .failure(let e): throw e
        }
    }
}

// MARK: - Bootstrap entry points

extension ClientBootstrap {
    public func connect(to endpoint: Endpoint) async throws(WendyNetError) -> Channel<Message> {
        let _ = security
        let _ = udpAssociationTimeoutSeconds

        let hostname: String
        let port: UInt16
        switch endpoint {
        case .ipHost(let host, let endpointPort):
            hostname = host
            port = endpointPort
        case .wendyPeer:
            throw .connectionFailed
        }

        let transport = TransportInfo(kind: .tcp, isStream: true)
        var closures = pipelineFactory()
        if let framerFactory = framerFactory {
            closures = framerFactory().composing(closures)
        }

        let context = ConnectionContext(remoteEndpoint: endpoint, transport: transport, security: security)
        closures.started(context)

        let nioAsyncChannel: NIOByteBufferChannel
        do {
            nioAsyncChannel = try await NIOPosix.ClientBootstrap(group: standardEventLoopGroup)
                .connect(host: hostname, port: Int(port)) { channel in
                    channel.eventLoop.makeCompletedFuture {
                        try NIOByteBufferChannel(wrappingChannelSynchronously: channel)
                    }
                }
        } catch is CancellationError {
            throw .cancelled
        } catch {
            throw .connectionFailed
        }

        let core = ChannelCore<Message>(
            nioAsyncChannel: nioAsyncChannel,
            endpoint: endpoint,
            transport: transport,
            decode: closures.decode,
            encode: closures.encode
        )

        return Channel<Message>(
            endpoint: endpoint,
            transport: transport,
            maximumMessageLength: wendyNetMaximumMessageLength,
            core: core
        )
    }
}

extension ServerBootstrap {
    public func bind(port: UInt16) async throws(WendyNetError) -> Listener<Message> {
        let _ = wendyNet
        let _ = udpAssociationTimeoutSeconds

        let transport = TransportInfo(kind: .tcp, isStream: true)
        let endpoint = Endpoint.ipHost(hostname: "0.0.0.0", port: port)
        let context = ConnectionContext(remoteEndpoint: endpoint, transport: transport, security: security)

        let pipelineFactory = self.pipelineFactory
        let framerFactory = self.framerFactory

        let serverChannel: NIOServerInboundChannel
        do {
            serverChannel = try await NIOPosix.ServerBootstrap(group: standardEventLoopGroup)
                .serverChannelOption(ChannelOptions.backlog, value: 4)
                .bind(host: "0.0.0.0", port: Int(port)) { childChannel in
                    childChannel.eventLoop.makeCompletedFuture {
                        try NIOByteBufferChannel(wrappingChannelSynchronously: childChannel)
                    }
                }
        } catch is CancellationError {
            throw .cancelled
        } catch {
            throw .listenerError
        }

        guard let resolvedPort = serverChannel.channel.localAddress?.port.map(UInt16.init) else {
            throw .listenerError
        }

        let core = ListenerCore<Message>(
            port: resolvedPort,
            context: context,
            serverChannel: serverChannel,
            pipelineFactory: pipelineFactory,
            framerFactory: framerFactory
        )
        return Listener<Message>(port: resolvedPort, core: core)
    }
}

#endif
