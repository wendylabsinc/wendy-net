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

fileprivate final class _LockedBox<T>: @unchecked Sendable {
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

private let standardEventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)

fileprivate typealias NIOByteBufferChannel = NIOAsyncChannel<ByteBuffer, ByteBuffer>
fileprivate typealias NIOServerInboundChannel = NIOAsyncChannel<NIOByteBufferChannel, Never>

// MARK: - Inbound bridge

private final class _ByteBufferInboundBridge<Message: Sendable>: Sendable {
    private struct State {
        // `iterator` is briefly set to nil while a (single) caller is awaiting
        // the next NIO buffer outside the lock. A second concurrent `next()`
        // call seeing nil here surfaces `.concurrentAccess` to the caller.
        var iterator: NIOAsyncChannelInboundStream<ByteBuffer>.AsyncIterator?
        var pending: [Message] = []
        var error: WendyNetError? = nil
        var ended = false
        var decode: (ByteBuffer, (Message) -> Void, (WendyNetError) -> Void) -> Void
    }
    private let state: _LockedBox<State>

    init(
        iterator: NIOAsyncChannelInboundStream<ByteBuffer>.AsyncIterator,
        decode: @escaping (ByteBuffer, (Message) -> Void, (WendyNetError) -> Void) -> Void
    ) {
        self.state = _LockedBox(State(iterator: iterator, decode: decode))
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

            // Claim the iterator out of the box. If it's already nil another
            // caller is awaiting -- single-consumer contract violated; surface
            // as `.concurrentAccess`.
            let claimed: NIOAsyncChannelInboundStream<ByteBuffer>.AsyncIterator? = state.withLockedValue { s in
                let i = s.iterator
                s.iterator = nil
                return i
            }
            guard var iter = claimed else {
                return .failure(.concurrentAccess)
            }

            // Await outside the lock.
            let pull: Result<ByteBuffer?, Error>
            do {
                let buf = try await iter.next()
                pull = .success(buf)
            } catch {
                pull = .failure(error)
            }

            let result: InboundStep<Message>? = state.withLockedValue { s in
                s.iterator = iter
                switch pull {
                case .success(let bufOpt):
                    guard let buf = bufOpt else {
                        s.ended = true
                        return .end
                    }
                    // Zero-copy hand-off: pipeline sees NIO's buffer directly.
                    s.decode(buf, { msg in
                        s.pending.append(msg)
                    }, { err in
                        s.error = err
                    })
                    if let err = s.error { return .failure(err) }
                    if !s.pending.isEmpty { return .message(s.pending.removeFirst()) }
                    return nil  // loop to deliver from buffered state
                case .failure(let err):
                    s.ended = true
                    if err is CancellationError {
                        return .failure(.cancelled)
                    }
                    return .failure(.connectionFailed)
                }
            }
            if let result { return result }
        }
    }
}

// MARK: - Outbound bridge

private final class _ByteBufferOutboundBridge<Message: Sendable>: Sendable {
    private struct State {
        var encode: (Message) -> ByteBuffer
    }
    private let state: _LockedBox<State>
    fileprivate let nioOutbound: NIOAsyncChannelOutboundWriter<ByteBuffer>

    init(
        nioOutbound: NIOAsyncChannelOutboundWriter<ByteBuffer>,
        encode: @escaping (Message) -> ByteBuffer
    ) {
        self.nioOutbound = nioOutbound
        self.state = _LockedBox(State(encode: encode))
    }

    func write(_ msg: Message) async -> OutboundStep {
        // Zero-copy hand-off: pipeline-produced buffer goes straight to NIO.
        let buf = state.withLockedValue { s in s.encode(msg) }
        do {
            try await nioOutbound.write(buf)
        } catch is CancellationError {
            return .failure(.cancelled)
        } catch {
            return .failure(.connectionFailed)
        }
        return .accepted(.accepted)
    }
}

// MARK: - Accept bridge

private final class _AcceptIteratorBox<Message: Sendable>: Sendable {
    private struct State {
        // `iterator` is briefly nil while a (single) caller is awaiting the
        // next accepted connection outside the lock; a second concurrent call
        // seeing nil here surfaces `.concurrentAccess` to the caller.
        var iterator: NIOAsyncChannelInboundStream<NIOByteBufferChannel>.AsyncIterator?
        var ended = false
        var error: WendyNetError? = nil
    }
    private let state: _LockedBox<State>
    private let context: ConnectionContext
    private let pipelineFactory: @Sendable () -> _PipelineClosures<Message>
    private let framerFactory: (@Sendable () -> _FramerClosures)?

    init(
        iterator: NIOAsyncChannelInboundStream<NIOByteBufferChannel>.AsyncIterator,
        context: ConnectionContext,
        pipelineFactory: @escaping @Sendable () -> _PipelineClosures<Message>,
        framerFactory: (@Sendable () -> _FramerClosures)?
    ) {
        self.state = _LockedBox(State(iterator: iterator))
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
// Sendable. The non-Sendable pipeline closures live inside `_LockedBox<Closures?>`
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
    private let closures: _LockedBox<Closures?>

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
        self.closures = _LockedBox(Closures(decode: decode, encode: encode))
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
                let inboundBridge = _ByteBufferInboundBridge<Message>(
                    iterator: nioInbound.makeAsyncIterator(),
                    decode: decodeClosure
                )
                let outboundBridge = _ByteBufferOutboundBridge<Message>(
                    nioOutbound: nioOutbound,
                    encode: encodeClosure
                )

                let inbound = Inbound<Message>(_next: { [inboundBridge] in
                    await inboundBridge.next()
                })
                let outbound = Outbound<Message>(_write: { [outboundBridge] msg in
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
    private let pipelineFactory: @Sendable () -> _PipelineClosures<Message>
    private let framerFactory: (@Sendable () -> _FramerClosures)?
    /// Single-use latch -- see `executeThenClose` for rationale.
    private let used = _LockedBox<Bool>(false)

    fileprivate init(
        port: UInt16,
        context: ConnectionContext,
        serverChannel: NIOServerInboundChannel,
        pipelineFactory: @escaping @Sendable () -> _PipelineClosures<Message>,
        framerFactory: (@Sendable () -> _FramerClosures)?
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
                let bridge = _AcceptIteratorBox<Message>(
                    iterator: acceptStream.makeAsyncIterator(),
                    context: context,
                    pipelineFactory: pipelineFactory,
                    framerFactory: framerFactory
                )
                let accepted = Accepted<Message>(_next: { [bridge] in
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
        var closures = _pipelineFactory()
        if let framerFactory = _framerFactory {
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

        let pipelineFactory = self._pipelineFactory
        let framerFactory = self._framerFactory

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

        let core = ListenerCore<Message>(
            port: port,
            context: context,
            serverChannel: serverChannel,
            pipelineFactory: pipelineFactory,
            framerFactory: framerFactory
        )
        return Listener<Message>(port: port, core: core)
    }
}

#endif
