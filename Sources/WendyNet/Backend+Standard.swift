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

// MARK: - Parked-waiter helper
//
// The single-parked-continuation pattern shared by the conduit reader and the
// hub's accept iterator: poll for a ready value under the state lock, else park
// one continuation (a second concurrent caller is rejected via `park`), with
// cancellation taking and resuming the parked waiter.
//
//   poll:         ready value, or nil to park
//   park:         store the continuation, or return a value (e.g. concurrentAccess)
//   cancelWaiter: take and clear the parked continuation
private func parkOrResume<S, R: Sendable>(
    _ box: LockedBox<S>,
    cancelled: R,
    poll: @escaping @Sendable (inout S) -> R?,
    park: @escaping @Sendable (inout S, CheckedContinuation<R, Never>) -> R?,
    cancelWaiter: @escaping @Sendable (inout S) -> CheckedContinuation<R, Never>?
) async -> R {
    if let fast = box.withLockedValue({ poll(&$0) }) { return fast }
    return await withTaskCancellationHandler(operation: {
        await withCheckedContinuation { (continuation: CheckedContinuation<R, Never>) in
            let now: R? = box.withLockedValue { s in
                if Task.isCancelled { return cancelled }
                if let r = poll(&s) { return r }
                return park(&s, continuation)
            }
            if let now { continuation.resume(returning: now) }
        }
    }, onCancel: {
        let waiter = box.withLockedValue { cancelWaiter(&$0) }
        waiter?.resume(returning: cancelled)
    })
}

private let standardEventLoopGroup = MultiThreadedEventLoopGroup.singleton

fileprivate typealias NIOByteBufferChannel = NIOAsyncChannel<ByteBuffer, ByteBuffer>
fileprivate typealias NIOServerInboundChannel = NIOAsyncChannel<NIOByteBufferChannel, Never>
fileprivate typealias NIODatagramChannel = NIOAsyncChannel<AddressedEnvelope<ByteBuffer>, AddressedEnvelope<ByteBuffer>>

// MARK: - Connected-UDP unwrap handler

private final class DatagramUnwrapHandler: ChannelInboundHandler {
    typealias InboundIn = AddressedEnvelope<ByteBuffer>
    typealias InboundOut = ByteBuffer

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let envelope = unwrapInboundIn(data)
        context.fireChannelRead(wrapInboundOut(envelope.data))
    }
}

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

// MARK: - UDP peer conduit
//
// A per-peer association on a shared (unconnected) datagram listener. The
// listener's demux pump routes each inbound datagram to the conduit matching
// its source address; writes go back out through the shared channel's writer
// wrapped in an `AddressedEnvelope` carrying this conduit's remote address.

fileprivate final class UDPPeerConduit: Sendable {
    let remote: SocketAddress
    private let writer: NIOAsyncChannelOutboundWriter<AddressedEnvelope<ByteBuffer>>
    /// Removes this association from the hub's demux map, so a later datagram
    /// from the same source spawns a fresh accepted channel.
    private let onClose: @Sendable (SocketAddress) -> Void

    private struct State {
        var pending: [ByteBuffer] = []
        // Briefly holds the (single) parked reader. A second concurrent
        // `next()` observing a non-nil waiter gets `.concurrentAccess`.
        var waiter: CheckedContinuation<Result<ByteBuffer?, WendyNetError>, Never>? = nil
        var ended = false
        var error: WendyNetError? = nil
        var lastActivity: ContinuousClock.Instant
    }
    private let state: LockedBox<State>

    init(
        remote: SocketAddress,
        writer: NIOAsyncChannelOutboundWriter<AddressedEnvelope<ByteBuffer>>,
        onClose: @escaping @Sendable (SocketAddress) -> Void
    ) {
        self.remote = remote
        self.writer = writer
        self.onClose = onClose
        self.state = LockedBox(State(lastActivity: ContinuousClock().now))
    }

    /// Time left before the idle deadline (`timeout` since the last datagram):
    /// `nil` once the association has ended, `<= .zero` once it has expired.
    func idleRemaining(timeout: Duration) -> Duration? {
        state.withLockedValue { s in
            s.ended ? nil : timeout - (ContinuousClock().now - s.lastActivity)
        }
    }

    /// Called by the demux pump when a datagram arrives from `remote`.
    func deliver(_ buf: ByteBuffer) {
        let waiter: CheckedContinuation<Result<ByteBuffer?, WendyNetError>, Never>? =
            state.withLockedValue { s in
                if s.ended { return nil }  // association torn down: drop (UDP semantics)
                s.lastActivity = ContinuousClock().now
                if let w = s.waiter {
                    s.waiter = nil
                    return w
                }
                s.pending.append(buf)
                return nil
            }
        waiter?.resume(returning: .success(buf))
    }

    /// Terminal: the listener went away or the channel scope exited. Buffered
    /// datagrams already decoded upstream stay deliverable; the next read here
    /// observes end-of-stream (or `error`).
    func finish(_ error: WendyNetError?) {
        let waiter: CheckedContinuation<Result<ByteBuffer?, WendyNetError>, Never>? =
            state.withLockedValue { s in
                if s.ended { return nil }
                s.ended = true
                s.error = error
                let w = s.waiter
                s.waiter = nil
                return w
            }
        if let waiter {
            if let error {
                waiter.resume(returning: .failure(error))
            } else {
                waiter.resume(returning: .success(nil))
            }
        }
    }

    func next() async -> Result<ByteBuffer?, WendyNetError> {
        await parkOrResume(
            state,
            cancelled: Result<ByteBuffer?, WendyNetError>.failure(.cancelled),
            poll: { s in
                if !s.pending.isEmpty { return .success(s.pending.removeFirst()) }
                if let err = s.error { return .failure(err) }
                if s.ended { return .success(nil) }
                return nil
            },
            park: { s, continuation in
                if s.waiter != nil { return .failure(.concurrentAccess) }
                s.waiter = continuation
                return nil
            },
            cancelWaiter: { s in
                let w = s.waiter
                s.waiter = nil
                return w
            }
        )
    }

    func write(_ buf: ByteBuffer) async -> OutboundStep {
        let earlyError: WendyNetError? = state.withLockedValue { s in
            if let err = s.error { return err }
            if s.ended { return .closed }
            s.lastActivity = ContinuousClock().now
            return nil
        }
        if let earlyError { return .failure(earlyError) }
        do {
            try await writer.write(AddressedEnvelope(remoteAddress: remote, data: buf))
        } catch is CancellationError {
            return .failure(.cancelled)
        } catch {
            return .failure(.connectionFailed)
        }
        return .accepted
    }

    /// Channel scope exited: end the association and unhook from the demux map.
    func close() {
        finish(nil)
        onClose(remote)
    }
}

// MARK: - UDP demux hub
//
// Owns the per-source-address association map for one bound (unconnected)
// datagram listener. The first datagram from a new source spawns a
// `UDPPeerConduit` plus an accepted `Channel`; subsequent datagrams route to
// the existing conduit. Datagrams arriving after the hub has finished are
// dropped (acceptable UDP semantics).

fileprivate final class UDPDemuxHub<Message: Sendable>: Sendable {
    private struct State {
        var peers: [SocketAddress: UDPPeerConduit] = [:]
        var pendingChannels: [Channel<Message>] = []
        // Briefly holds the (single) parked accept() caller. A second
        // concurrent caller observing non-nil gets `.concurrentAccess`.
        var acceptWaiter: CheckedContinuation<AcceptedStep<Message>, Never>? = nil
        var ended = false
        var error: WendyNetError? = nil
    }
    private let state: LockedBox<State>
    private let context: ConnectionContext
    private let pipelineFactory: @Sendable () -> PipelineClosures<Message>
    private let writer: NIOAsyncChannelOutboundWriter<AddressedEnvelope<ByteBuffer>>
    /// Idle timeout applied to each accepted association (`.zero` disables).
    private let idleTimeout: Duration

    init(
        context: ConnectionContext,
        idleTimeout: Duration,
        pipelineFactory: @escaping @Sendable () -> PipelineClosures<Message>,
        writer: NIOAsyncChannelOutboundWriter<AddressedEnvelope<ByteBuffer>>
    ) {
        self.state = LockedBox(State())
        self.context = context
        self.idleTimeout = idleTimeout
        self.pipelineFactory = pipelineFactory
        self.writer = writer
    }

    /// Called only from the (single) demux pump task, so creation of a new
    /// association cannot race with itself.
    func deliver(_ envelope: AddressedEnvelope<ByteBuffer>) {
        let remote = envelope.remoteAddress

        let existing: UDPPeerConduit? = state.withLockedValue { s in s.peers[remote] }
        if let existing {
            existing.deliver(envelope.data)
            return
        }

        // New peer: spawn a conduit and an accepted channel around it. The
        // pipeline closures are constructed here -- once per association --
        // and immediately confined to the channel core's locked storage.
        let conduit = UDPPeerConduit(remote: remote, writer: writer, onClose: { [weak self] r in
            self?.removePeer(r)
        })
        let endpoint = Self.endpoint(for: remote)
        let peerContext = ConnectionContext(
            remoteEndpoint: endpoint,
            transport: context.transport,
            security: context.security
        )
        let closures = pipelineFactory()

        let core = ChannelCore<Message>(
            udpConduit: conduit,
            idleTimeout: idleTimeout,
            endpoint: endpoint,
            transport: context.transport,
            decode: closures.decode,
            encode: closures.encode
        )
        let channel = Channel<Message>(
            endpoint: endpoint,
            transport: context.transport,
            maximumMessageLength: wendyNetMaximumMessageLength,
            core: core
        )

        let (waiter, dropped): (CheckedContinuation<AcceptedStep<Message>, Never>?, Bool) =
            state.withLockedValue { s in
                if s.ended { return (nil, true) }
                s.peers[remote] = conduit
                if let w = s.acceptWaiter {
                    s.acceptWaiter = nil
                    return (w, false)
                }
                s.pendingChannels.append(channel)
                return (nil, false)
            }
        if dropped { return }
        closures.started(peerContext)
        conduit.deliver(envelope.data)
        waiter?.resume(returning: .channel(channel))
    }

    func nextAccepted() async -> AcceptedStep<Message> {
        await parkOrResume(
            state,
            cancelled: AcceptedStep<Message>.failure(.cancelled),
            poll: { s in
                if !s.pendingChannels.isEmpty { return .channel(s.pendingChannels.removeFirst()) }
                if let err = s.error { return .failure(err) }
                if s.ended { return .end }
                return nil
            },
            park: { s, continuation in
                if s.acceptWaiter != nil { return .failure(.concurrentAccess) }
                s.acceptWaiter = continuation
                return nil
            },
            cancelWaiter: { s in
                let w = s.acceptWaiter
                s.acceptWaiter = nil
                return w
            }
        )
    }

    /// Terminal: the listener scope is unwinding (or the pump hit an error).
    /// First call wins; ends every live association.
    func finish(_ error: WendyNetError?) {
        let (waiter, conduits): (CheckedContinuation<AcceptedStep<Message>, Never>?, [UDPPeerConduit]) =
            state.withLockedValue { s in
                if s.ended { return (nil, []) }
                s.ended = true
                s.error = error
                let w = s.acceptWaiter
                s.acceptWaiter = nil
                let cs = Array(s.peers.values)
                s.peers.removeAll()
                return (w, cs)
            }
        if let waiter {
            if let error {
                waiter.resume(returning: .failure(error))
            } else {
                waiter.resume(returning: .end)
            }
        }
        for conduit in conduits {
            conduit.finish(error)
        }
    }

    private func removePeer(_ remote: SocketAddress) {
        state.withLockedValue { s in
            _ = s.peers.removeValue(forKey: remote)
        }
    }

    private static func endpoint(for address: SocketAddress) -> Endpoint {
        .ipHost(hostname: address.ipAddress ?? "?", port: UInt16(address.port ?? 0))
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

fileprivate enum ChannelBacking {
    /// TCP connection or connected-UDP client: a dedicated NIO channel.
    case nio(NIOByteBufferChannel)
    /// Per-peer association on a shared datagram listener, with its idle
    /// timeout (`.zero` disables idle reaping).
    case udpPeer(UDPPeerConduit, idleTimeout: Duration)
}

final class ChannelCore<Message: Sendable>: Sendable {
    let endpoint: Endpoint
    let transport: TransportInfo
    private let backing: ChannelBacking
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
        self.backing = .nio(nioAsyncChannel)
        self.endpoint = endpoint
        self.transport = transport
        self.closures = LockedBox(Closures(decode: decode, encode: encode))
    }

    fileprivate init(
        udpConduit: UDPPeerConduit,
        idleTimeout: Duration,
        endpoint: Endpoint,
        transport: TransportInfo,
        decode: @escaping (ByteBuffer, (Message) -> Void, (WendyNetError) -> Void) -> Void,
        encode: @escaping (Message) -> ByteBuffer
    ) {
        self.backing = .udpPeer(udpConduit, idleTimeout: idleTimeout)
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

        switch backing {
        case .nio(let nioAsyncChannel):
            return try await executeThenCloseNIO(
                nioAsyncChannel,
                decode: decodeClosure,
                encode: encodeClosure,
                body
            )
        case .udpPeer(let conduit, let idleTimeout):
            return try await executeThenCloseUDPPeer(
                conduit,
                idleTimeout: idleTimeout,
                decode: decodeClosure,
                encode: encodeClosure,
                body
            )
        }
    }

    private func executeThenCloseNIO<R: Sendable>(
        _ nioAsyncChannel: NIOByteBufferChannel,
        decode decodeClosure: @escaping (ByteBuffer, (Message) -> Void, (WendyNetError) -> Void) -> Void,
        encode encodeClosure: @escaping (Message) -> ByteBuffer,
        _ body: @Sendable (Inbound<Message>, Outbound<Message>) async throws(WendyNetError) -> R
    ) async throws(WendyNetError) -> R {
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

    private func executeThenCloseUDPPeer<R: Sendable>(
        _ conduit: UDPPeerConduit,
        idleTimeout: Duration,
        decode decodeClosure: @escaping (ByteBuffer, (Message) -> Void, (WendyNetError) -> Void) -> Void,
        encode encodeClosure: @escaping (Message) -> ByteBuffer,
        _ body: @Sendable (Inbound<Message>, Outbound<Message>) async throws(WendyNetError) -> R
    ) async throws(WendyNetError) -> R {
        let inboundBridge = InboundBridge<Message>(
            decode: decodeClosure,
            // Single-consumer contract is enforced by the conduit itself.
            pull: { [conduit] in await conduit.next() }
        )
        let outboundBridge = OutboundBridge<Message>(
            encode: encodeClosure,
            sink: { [conduit] buf in await conduit.write(buf) }
        )

        let inbound = Inbound<Message>(nextStep: { [inboundBridge] in
            await inboundBridge.next()
        })
        let outbound = Outbound<Message>(writeStep: { [outboundBridge] msg in
            await outboundBridge.write(msg)
        })

        // Run the body alongside the association's idle timer; cancelling the
        // group when the body returns tears the timer down with the channel.
        // If the timer fires first it ends the conduit, surfacing end-of-stream
        // to the body.
        let outcome: Result<R, WendyNetError> =
            await withTaskGroup(of: Void.self, returning: Result<R, WendyNetError>.self) { group in
                if idleTimeout > .zero {
                    group.addTask { [conduit] in
                        await runIdleTimer(
                            remaining: { conduit.idleRemaining(timeout: idleTimeout) },
                            sleep: { d in (try? await Task.sleep(for: d)) != nil },
                            evict: { conduit.close() }
                        )
                    }
                }
                let result: Result<R, WendyNetError>
                do throws(WendyNetError) {
                    result = .success(try await body(inbound, outbound))
                } catch {
                    result = .failure(error)
                }
                conduit.close()
                group.cancelAll()
                return result
            }
        switch outcome {
        case .success(let v): return v
        case .failure(let e): throw e
        }
    }
}

// MARK: - ListenerCore


fileprivate enum ListenerBacking {
    /// TCP: a NIO server channel yielding one child channel per connection.
    case tcp(NIOServerInboundChannel)
    /// UDP: a single bound datagram channel; associations are demultiplexed
    /// by source address via `UDPDemuxHub`.
    case udp(NIODatagramChannel)
}

final class ListenerCore<Message: Sendable>: Sendable {
    let port: UInt16
    let context: ConnectionContext
    private let backing: ListenerBacking
    private let udpAssociationTimeout: Duration
    private let pipelineFactory: @Sendable () -> PipelineClosures<Message>
    private let framerFactory: (@Sendable () -> FramerClosures)?
    /// Single-use latch -- see `executeThenClose` for rationale.
    private let used = LockedBox<Bool>(false)

    fileprivate init(
        port: UInt16,
        context: ConnectionContext,
        backing: ListenerBacking,
        udpAssociationTimeout: Duration,
        pipelineFactory: @escaping @Sendable () -> PipelineClosures<Message>,
        framerFactory: (@Sendable () -> FramerClosures)?
    ) {
        self.port = port
        self.context = context
        self.backing = backing
        self.udpAssociationTimeout = udpAssociationTimeout
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

        switch backing {
        case .tcp(let serverChannel):
            return try await executeThenCloseTCP(serverChannel, body)
        case .udp(let datagramChannel):
            return try await executeThenCloseUDP(datagramChannel, body)
        }
    }

    private func executeThenCloseTCP<R: Sendable>(
        _ serverChannel: NIOServerInboundChannel,
        _ body: @Sendable (Accepted<Message>) async throws(WendyNetError) -> R
    ) async throws(WendyNetError) -> R {
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

    private func executeThenCloseUDP<R: Sendable>(
        _ datagramChannel: NIODatagramChannel,
        _ body: @Sendable (Accepted<Message>) async throws(WendyNetError) -> R
    ) async throws(WendyNetError) -> R {
        let context = self.context
        let pipelineFactory = self.pipelineFactory
        let timeout = self.udpAssociationTimeout
        // Framers are not applied: datagrams already carry message boundaries.

        let outcome: Result<R, WendyNetError>
        do {
            outcome = try await datagramChannel.executeThenClose { inbound, outbound -> Result<R, WendyNetError> in
                // Each accepted association idle-times-out within its own
                // executeThenClose scope (see ChannelCore), so the hub only
                // demultiplexes — no central reaper.
                let hub = UDPDemuxHub<Message>(
                    context: context,
                    idleTimeout: timeout,
                    pipelineFactory: pipelineFactory,
                    writer: outbound
                )
                // The demux pump runs as a structured child of this scope; the
                // accept iterator hands out per-peer channels spawned by the hub.
                return await withTaskGroup(of: Void.self, returning: Result<R, WendyNetError>.self) { group in
                    group.addTask { [hub] in
                        do {
                            for try await envelope in inbound {
                                hub.deliver(envelope)
                            }
                            hub.finish(nil)
                        } catch is CancellationError {
                            hub.finish(nil)
                        } catch {
                            hub.finish(.listenerError)
                        }
                    }

                    let accepted = Accepted<Message>(nextStep: { [hub] in
                        await hub.nextAccepted()
                    })

                    let result: Result<R, WendyNetError>
                    do throws(WendyNetError) {
                        result = .success(try await body(accepted))
                    } catch {
                        result = .failure(error)
                    }
                    // First finish wins: if the pump already finished (socket
                    // error), this is a no-op; otherwise it ends every live
                    // association before the pump is torn down.
                    hub.finish(nil)
                    group.cancelAll()
                    return result
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

        let hostname: String
        let port: UInt16
        switch endpoint {
        case .ipHost(let host, let endpointPort):
            hostname = host
            port = endpointPort
        case .wendyPeer:
            throw .connectionFailed
        }

        let isStream = reliability == .reliable
        let transport = TransportInfo(kind: isStream ? .tcp : .udp, isStream: isStream)
        var closures = pipelineFactory()
        // Framers are only meaningful on stream transports; datagrams already
        // carry message boundaries.
        if let framerFactory = framerFactory, isStream {
            closures = framerFactory().composing(closures)
        }

        let context = ConnectionContext(remoteEndpoint: endpoint, transport: transport, security: security)

        let nioAsyncChannel: NIOByteBufferChannel
        do {
            if isStream {
                nioAsyncChannel = try await NIOPosix.ClientBootstrap(group: standardEventLoopGroup)
                    .connect(host: hostname, port: Int(port)) { channel in
                        channel.eventLoop.makeCompletedFuture {
                            try NIOByteBufferChannel(wrappingChannelSynchronously: channel)
                        }
                    }
            } else {
                // Connected UDP: the kernel filters inbound traffic to this
                // peer. The unwrap handler strips the AddressedEnvelope reads
                // so the async channel deals in plain ByteBuffers; writes are
                // plain ByteBuffers because connect() fixed the remote.
                nioAsyncChannel = try await NIOPosix.DatagramBootstrap(group: standardEventLoopGroup)
                    .connect(host: hostname, port: Int(port)) { channel in
                        channel.eventLoop.makeCompletedFuture {
                            try channel.pipeline.syncOperations.addHandler(DatagramUnwrapHandler())
                            return try NIOByteBufferChannel(wrappingChannelSynchronously: channel)
                        }
                    }
            }
        } catch is CancellationError {
            throw .cancelled
        } catch {
            throw .connectionFailed
        }

        closures.started(context)

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

        let isStream = reliability == .reliable
        let transport = TransportInfo(kind: isStream ? .tcp : .udp, isStream: isStream)
        let endpoint = Endpoint.ipHost(hostname: "0.0.0.0", port: port)
        let context = ConnectionContext(remoteEndpoint: endpoint, transport: transport, security: security)

        let pipelineFactory = self.pipelineFactory
        let framerFactory = self.framerFactory

        let backing: ListenerBacking
        do {
            if isStream {
                let serverChannel: NIOServerInboundChannel = try await NIOPosix.ServerBootstrap(group: standardEventLoopGroup)
                    .serverChannelOption(ChannelOptions.backlog, value: 4)
                    .bind(host: "0.0.0.0", port: Int(port)) { childChannel in
                        childChannel.eventLoop.makeCompletedFuture {
                            try NIOByteBufferChannel(wrappingChannelSynchronously: childChannel)
                        }
                    }
                backing = .tcp(serverChannel)
            } else {
                // Unconnected UDP: one bound datagram channel for the whole
                // listener; per-peer associations are demultiplexed by source
                // address inside ListenerCore's executeThenClose.
                let datagramChannel: NIODatagramChannel = try await NIOPosix.DatagramBootstrap(group: standardEventLoopGroup)
                    .bind(host: "0.0.0.0", port: Int(port)) { channel in
                        channel.eventLoop.makeCompletedFuture {
                            try NIODatagramChannel(wrappingChannelSynchronously: channel)
                        }
                    }
                backing = .udp(datagramChannel)
            }
        } catch is CancellationError {
            throw .cancelled
        } catch {
            throw .listenerError
        }

        let boundChannel: NIOCore.Channel
        switch backing {
        case .tcp(let serverChannel): boundChannel = serverChannel.channel
        case .udp(let datagramChannel): boundChannel = datagramChannel.channel
        }
        guard let resolvedPort = boundChannel.localAddress?.port.map(UInt16.init) else {
            throw .listenerError
        }

        let core = ListenerCore<Message>(
            port: resolvedPort,
            context: context,
            backing: backing,
            udpAssociationTimeout: udpAssociationTimeout,
            pipelineFactory: pipelineFactory,
            framerFactory: framerFactory
        )
        return Listener<Message>(port: resolvedPort, core: core)
    }
}

#endif
