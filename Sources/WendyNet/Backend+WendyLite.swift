#if WendyNetBackendWendyLite
import CWendyNet
import WendyLite
import Synchronization
import _Concurrency

// MARK: - Internal lock primitive
//
// `LockedBox<T>` is a lock-protected carrier for mutable state. It plays
// the combined role of SwiftNIO's `NIOLockedValueBox` (lock-protected
// mutation of `Sendable` state) and `NIOLoopBoundBox` (carrier of
// non-`Sendable` state confined by external discipline). The second role
// is what makes this version load-bearing for WendyNet.
//
// Users implement `PipelineStage` as plain non-`Sendable` classes (mirroring
// SwiftNIO's `ChannelHandler`). The framework captures their `decode`/`encode`
// into non-`@Sendable` closures and stores them inside this box, so the box
// must accept non-`Sendable` `T`. Because the lock serialises every entry to
// those closures, the user's stage methods are invoked from at most one task
// at a time -- providing the same effective guarantee SwiftNIO's `EventLoop`
// provides to its handlers.
//
// The hazard `@unchecked Sendable` does NOT protect against: extracting the
// value out of `withLockedValue` and using it from outside the lock. Don't do
// that with non-`Sendable` `T`. Audit any `withLockedValue { ... return x }`
// where `x` is non-`Sendable`.
//
// ## Why not `Synchronization.Mutex`?
//
// `Mutex.withLock` returns `sending Result`, which is incompatible with
// returning non-`Sendable` values out of the locked region -- defeating our
// ability to wrap non-`Sendable` `T`. The atomic-spinlock pattern has no such
// constraint.
//
// WendyLite ships a sibling copy of this type for its own internal use over
// in `wendy-lite/Sources/WendyLite/Internal.swift`. That copy only wraps
// `Sendable` state and will move to `Mutex` once Swift 6.4 ships it for the
// embedded wasm SDK; this copy stays because we genuinely need the non-
// `Sendable`-T capability.
//
// ## Lock implementation
//
// `Synchronization.Atomic<Bool>` spinlock. On WASM's single-threaded
// cooperative executor the spin loop will never iterate -- there is no other
// thread to contend with, and synchronous code cannot interleave at the lock
// boundary. The lock is logically a no-op there, but writing it as a real
// lock (rather than no synchronization at all) means the `@unchecked Sendable`
// claim is grounded in lock discipline, not in "trust the executor": if WASI
// ever gains pre-emptive scheduling or multi-threaded executors, the code
// still works.

fileprivate final class LockedBox<T>: @unchecked Sendable {
    private let locked = Atomic<Bool>(false)
    private var value: T

    init(_ value: T) {
        self.value = value
    }

    func withLockedValue<R, E: Error>(_ body: (inout T) throws(E) -> R) throws(E) -> R {
        while !locked.compareExchange(
            expected: false,
            desired: true,
            ordering: .acquiring
        ).exchanged {
            // Spin. Unreachable on single-threaded cooperative executors.
        }
        defer { locked.store(false, ordering: .releasing) }
        return try body(&value)
    }
}

/// Guest-side UDP receive buffer. Must be >= the host's CONFIG_WENDY_NET_BUFFER_SIZE.
private let wendyNetDatagramBufferSize = 1500

// MARK: - Parked-waiter helper
//
// The single-parked-continuation pattern shared by the listener's accept loop
// and the channel's receive loop: poll for a ready value under the state lock,
// else park one continuation (a second concurrent caller is rejected via
// `park`), with cancellation taking and resuming the parked waiter. External
// events (host drain, close) resume the same waiter field directly.
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

private let wendyNetCallbackHandlerID: Int32 = 2
private let wendyNetEventAcceptReady: Int32 = 1
private let wendyNetEventReadReady: Int32 = 2
private let wendyNetEventWriteReady: Int32 = 4
private let wendyNetEventClosed: Int32 = 8
private let wendyNetEventError: Int32 = 16
private let wendyNetStatusReadable: Int32 = 1
private let wendyNetStatusWritable: Int32 = 2
private let wendyNetStatusClosed: Int32 = 4
private let wendyNetStatusError: Int32 = 8

private func registerWendyNetCallback() {
    CallbackDispatch.register(wendyNetCallbackHandlerID) { bits, _, _ in
        WendyNetState.shared.networkEvent(bits: bits)
    }
}

private enum WendyNetNative {
    @discardableResult
    static func initialize() -> Int32 {
        wendynet_init(wendyNetCallbackHandlerID)
    }

    static func drainEvents() -> Int32 {
        wendynet_drain_events()
    }

    static func listen(port: UInt16, backlog: Int32) -> Int32 {
        wendynet_tcp_listen(Int32(port), backlog)
    }

    /// Marshal a hostname into the `(const char *, int32 len)` pair the host
    /// imports expect. Returns -1 for an empty string (no base address).
    private static func withHostname(
        _ hostname: String,
        _ body: (UnsafePointer<CChar>, Int32) -> Int32
    ) -> Int32 {
        let bytes = Array(hostname.utf8)
        return bytes.withUnsafeBufferPointer { ptr in
            guard let baseAddress = ptr.baseAddress else { return -1 }
            return body(
                UnsafeRawPointer(baseAddress).assumingMemoryBound(to: CChar.self),
                Int32(ptr.count)
            )
        }
    }

    static func connect(hostname: String, port: UInt16) -> Int32 {
        withHostname(hostname) { ptr, len in
            wendynet_tcp_connect(ptr, len, Int32(port))
        }
    }

    static func udpListen(port: UInt16) -> Int32 {
        wendynet_udp_listen(Int32(port))
    }

    static func udpConnect(hostname: String, port: UInt16) -> Int32 {
        withHostname(hostname) { ptr, len in
            wendynet_udp_connect(ptr, len, Int32(port))
        }
    }

    static func accept(listenerHandle: Int32) -> Int32 {
        wendynet_listener_accept(listenerHandle)
    }

    @discardableResult
    static func closeListener(_ handle: Int32) -> Int32 {
        wendynet_listener_close(handle)
    }

    static func listenerPort(_ handle: Int32) -> Int32 {
        wendynet_listener_port(handle)
    }

    static func socketStatus(_ handle: Int32) -> Int32 {
        wendynet_socket_status(handle)
    }

    static func recv(socketHandle: Int32, into buffer: inout [UInt8]) -> Int32 {
        buffer.withUnsafeMutableBufferPointer { ptr in
            guard let baseAddress = ptr.baseAddress else { return -1 }
            return wendynet_socket_recv(
                socketHandle,
                UnsafeMutableRawPointer(baseAddress).assumingMemoryBound(to: CChar.self),
                Int32(ptr.count)
            )
        }
    }

    static func send(socketHandle: Int32, pointer: UnsafeRawPointer, count: Int) -> Int32 {
        wendynet_socket_send(
            socketHandle,
            pointer.assumingMemoryBound(to: CChar.self),
            Int32(count)
        )
    }

    @discardableResult
    static func closeSocket(_ handle: Int32) -> Int32 {
        wendynet_socket_close(handle)
    }
}

// MARK: - Runtime State

fileprivate protocol AnyListenerCore: AnyObject, Sendable {
    var handle: Int32 { get }
    func drainAccepted()
}

fileprivate protocol AnyChannelCore: AnyObject, Sendable {
    var handle: Int32 { get }
    func drainReadable()
    func notifyWritableOrClosed()
}

fileprivate enum WendyNetState {
    static let shared = WendyNetHub()
}

fileprivate final class WendyNetHub: Sendable {
    private struct State {
        var listeners: [AnyListenerCore] = []
        var channels: [AnyChannelCore] = []
        var isDraining = false
        var initialized = false
    }
    private let state = LockedBox(State())

    func ensureInitialized() -> Bool {
        state.withLockedValue { s in
            if s.initialized { return true }
            registerWendyNetCallback()
            let ok = WendyNetNative.initialize() == 0
            s.initialized = ok
            return ok
        }
    }

    func register(listener: AnyListenerCore) {
        state.withLockedValue { s in s.listeners.append(listener) }
    }

    func unregister(listenerHandle: Int32) {
        state.withLockedValue { s in s.listeners.removeAll { $0.handle == listenerHandle } }
    }

    func register(channel: AnyChannelCore) {
        state.withLockedValue { s in s.channels.append(channel) }
    }

    func unregister(channelHandle: Int32) {
        state.withLockedValue { s in s.channels.removeAll { $0.handle == channelHandle } }
    }

    func networkEvent(bits: Int32) {
        let _ = bits
        Task { [self] in
            drainNativeEvents()
        }
    }

    private func drainNativeEvents() {
        let canDrain: Bool = state.withLockedValue { s in
            if s.isDraining { return false }
            s.isDraining = true
            return true
        }
        if !canDrain { return }
        defer { state.withLockedValue { s in s.isDraining = false } }

        while true {
            let bits = WendyNetNative.drainEvents()
            if bits == 0 {
                break
            }

            // Snapshot the listener/channel lists so we don't hold the lock
            // while invoking their methods (those methods themselves take the
            // per-core locks and may resume continuations).
            let (listenersSnapshot, channelsSnapshot): ([AnyListenerCore], [AnyChannelCore]) =
                state.withLockedValue { s in (s.listeners, s.channels) }

            if (bits & wendyNetEventAcceptReady) != 0 {
                for listener in listenersSnapshot {
                    listener.drainAccepted()
                }
            }

            if (bits & (wendyNetEventReadReady | wendyNetEventClosed | wendyNetEventError)) != 0 {
                for channel in channelsSnapshot {
                    channel.drainReadable()
                }
            }

            if (bits & (wendyNetEventWriteReady | wendyNetEventClosed | wendyNetEventError)) != 0 {
                for channel in channelsSnapshot {
                    channel.notifyWritableOrClosed()
                }
            }
        }
    }
}

// MARK: - ListenerCore

final class ListenerCore<Message: Sendable>: AnyListenerCore, Sendable {
    let handle: Int32
    let port: UInt16
    let context: ConnectionContext
    /// Idle timeout applied to each accepted UDP association (`.zero` for TCP,
    /// which disables it).
    let udpAssociationTimeout: Duration
    // A fresh pipeline is built per accepted connection so peers never share
    // stage state.
    private let pipelineFactory: @Sendable () -> PipelineClosures<Message>
    private let framerFactory: (@Sendable () -> FramerClosures)?
    private let isStream: Bool
    private struct State {
        var pendingChannels: [Channel<Message>] = []
        var acceptWaiter: CheckedContinuation<Result<Channel<Message>?, WendyNetError>, Never>? = nil
        var isClosed = false
        /// Single-use latch -- see `executeThenClose` for rationale.
        var executeThenCloseUsed = false
    }
    private let state: LockedBox<State>

    init(
        handle: Int32,
        port: UInt16,
        context: ConnectionContext,
        udpAssociationTimeout: Duration,
        isStream: Bool,
        pipelineFactory: @escaping @Sendable () -> PipelineClosures<Message>,
        framerFactory: (@Sendable () -> FramerClosures)?
    ) {
        self.handle = handle
        self.port = port
        self.context = context
        self.udpAssociationTimeout = udpAssociationTimeout
        self.isStream = isStream
        self.pipelineFactory = pipelineFactory
        self.framerFactory = framerFactory
        self.state = LockedBox(State())
    }

    private func accept() async throws(WendyNetError) -> Channel<Message>? {
        // Fast path
        let fast: Result<Channel<Message>?, WendyNetError>? = state.withLockedValue { s in
            if !s.pendingChannels.isEmpty { return .success(s.pendingChannels.removeFirst()) }
            if s.isClosed { return .success(nil) }
            return nil
        }
        if let fast {
            switch fast {
            case .success(let c): return c
            case .failure(let e): throw e
            }
        }

        drainAccepted()

        let result = await parkOrResume(
            state,
            cancelled: Result<Channel<Message>?, WendyNetError>.failure(.cancelled),
            poll: { s in
                if !s.pendingChannels.isEmpty { return .success(s.pendingChannels.removeFirst()) }
                if s.isClosed { return .success(nil) }
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
        switch result {
        case .success(let channel): return channel
        case .failure(let error): throw error
        }
    }

    func executeThenClose<R: Sendable>(
        _ body: @Sendable (Accepted<Message>) async throws(WendyNetError) -> R
    ) async throws(WendyNetError) -> R {
        // Atomic check-and-set: exactly one caller wins; the other throws.
        let wasAlreadyUsed: Bool = state.withLockedValue { s in
            let prev = s.executeThenCloseUsed
            s.executeThenCloseUsed = true
            return prev
        }
        if wasAlreadyUsed {
            throw .alreadyConsumed
        }

        let accepted = Accepted<Message>(nextStep: { [self] in
            do throws(WendyNetError) {
                if let channel = try await accept() {
                    return .channel(channel)
                }
                return .end
            } catch {
                return .failure(error)
            }
        })
        let outcome: Result<R, WendyNetError>
        do throws(WendyNetError) {
            let value = try await body(accepted)
            outcome = .success(value)
        } catch {
            outcome = .failure(error)
        }
        await close()
        switch outcome {
        case .success(let v): return v
        case .failure(let e): throw e
        }
    }

    func drainAccepted() {
        let alreadyClosed: Bool = state.withLockedValue { s in s.isClosed }
        if alreadyClosed { return }

        while true {
            let socketHandle = WendyNetNative.accept(listenerHandle: handle)
            if socketHandle > 0 {
                var closures = pipelineFactory()
                if let framerFactory, isStream {
                    closures = framerFactory().composing(closures)
                }
                let core = ChannelCore<Message>(
                    handle: socketHandle,
                    endpoint: context.remoteEndpoint,
                    transport: context.transport,
                    idleTimeout: udpAssociationTimeout,
                    decode: closures.decode,
                    encode: closures.encode
                )
                let channel = Channel<Message>(
                    endpoint: context.remoteEndpoint,
                    transport: context.transport,
                    maximumMessageLength: wendyNetMaximumMessageLength,
                    core: core
                )

                let (resume, dropped): (CheckedContinuation<Result<Channel<Message>?, WendyNetError>, Never>?, Bool) =
                    state.withLockedValue { s in
                        if s.isClosed { return (nil, true) }
                        if let waiter = s.acceptWaiter {
                            s.acceptWaiter = nil
                            return (waiter, false)
                        }
                        s.pendingChannels.append(channel)
                        return (nil, false)
                    }
                if dropped {
                    WendyNetNative.closeSocket(socketHandle)
                    continue
                }
                closures.started(context)
                WendyNetState.shared.register(channel: core)
                resume?.resume(returning: .success(channel))
                continue
            }
            if socketHandle < 0 {
                closeWithError(.listenerError)
            }
            break
        }
    }

    private func close() async {
        let (waiter, didFirstClose): (
            CheckedContinuation<Result<Channel<Message>?, WendyNetError>, Never>?,
            Bool
        ) = state.withLockedValue { s in
            if s.isClosed { return (nil, false) }
            s.isClosed = true
            let w = s.acceptWaiter
            s.acceptWaiter = nil
            return (w, true)
        }
        if didFirstClose {
            WendyNetNative.closeListener(handle)
            WendyNetState.shared.unregister(listenerHandle: handle)
        }
        waiter?.resume(returning: .success(nil))
    }

    private func closeWithError(_ error: WendyNetError) {
        let waiter: CheckedContinuation<Result<Channel<Message>?, WendyNetError>, Never>? =
            state.withLockedValue { s in
                s.isClosed = true
                let w = s.acceptWaiter
                s.acceptWaiter = nil
                return w
            }
        WendyNetState.shared.unregister(listenerHandle: handle)
        waiter?.resume(returning: .failure(error))
    }
}

// MARK: - ChannelCore

final class ChannelCore<Message: Sendable>: AnyChannelCore, Sendable {
    let handle: Int32
    let endpoint: Endpoint
    let transport: TransportInfo
    /// Idle timeout for a UDP-listener association (`.zero` for TCP / clients,
    /// which disables it).
    let idleTimeout: Duration
    private struct State {
        var decode: (ByteBuffer, (Message) -> Void, (WendyNetError) -> Void) -> Void
        var encode: (Message) -> ByteBuffer
        var decodedMessages: [Message] = []
        var receiveWaiter: CheckedContinuation<Result<Message?, WendyNetError>, Never>? = nil
        var writableWaiter: CheckedContinuation<Void, Never>? = nil
        var closed = false
        var error: WendyNetError? = nil
        /// Last inbound/outbound activity, for idle reaping.
        var lastActivity: WendyClock.Instant
        /// Single-use latch -- see `executeThenClose` for rationale.
        var executeThenCloseUsed = false
    }
    private let state: LockedBox<State>

    init(
        handle: Int32,
        endpoint: Endpoint,
        transport: TransportInfo,
        idleTimeout: Duration = .zero,
        decode: @escaping (ByteBuffer, (Message) -> Void, (WendyNetError) -> Void) -> Void,
        encode: @escaping (Message) -> ByteBuffer
    ) {
        self.handle = handle
        self.endpoint = endpoint
        self.transport = transport
        self.idleTimeout = idleTimeout
        self.state = LockedBox(State(decode: decode, encode: encode, lastActivity: WendyClock().now))
    }

    /// Time left before the idle deadline (`idleTimeout` since the last
    /// activity): `nil` once closed/errored, `<= .zero` once expired.
    private func idleRemaining() -> Duration? {
        state.withLockedValue { s in
            (s.closed || s.error != nil)
                ? nil
                : idleTimeout - s.lastActivity.duration(to: WendyClock().now)
        }
    }

    private func receive() async throws(WendyNetError) -> Message? {
        // Fast paths
        if let early = tryDeliverReceived() {
            switch early {
            case .success(let m): return m
            case .failure(let e): throw e
            }
        }
        drainReadable()

        let result = await parkOrResume(
            state,
            cancelled: Result<Message?, WendyNetError>.failure(.cancelled),
            poll: { s in
                if !s.decodedMessages.isEmpty { return .success(s.decodedMessages.removeFirst()) }
                if let err = s.error { return .failure(err) }
                if s.closed { return .success(nil) }
                return nil
            },
            park: { s, continuation in
                if s.receiveWaiter != nil { return .failure(.concurrentAccess) }
                s.receiveWaiter = continuation
                return nil
            },
            cancelWaiter: { s in
                let w = s.receiveWaiter
                s.receiveWaiter = nil
                return w
            }
        )
        switch result {
        case .success(let message): return message
        case .failure(let error): throw error
        }
    }

    private func tryDeliverReceived() -> Result<Message?, WendyNetError>? {
        state.withLockedValue { s in
            if !s.decodedMessages.isEmpty { return .success(s.decodedMessages.removeFirst()) }
            if let err = s.error { return .failure(err) }
            if s.closed { return .success(nil) }
            return nil
        }
    }

    private func send(_ message: Message) async throws(WendyNetError) {
        // Initial gating: error / closed / cancelled.
        let earlyError: WendyNetError? = state.withLockedValue { s in
            if let err = s.error { return err }
            if s.closed { return .closed }
            return nil
        }
        if let earlyError { throw earlyError }
        if Task.isCancelled { throw .cancelled }

        // Encode under lock -- the encode closure may mutate the user's pipeline
        // stage state. The resulting ByteBuffer holds its bytes in CoW storage,
        // so passing it out of the lock does not copy.
        let buffer = state.withLockedValue { s in s.encode(message) }

        // Cancellation: close the channel so any in-flight syscall observes EBADF
        // and the parked writable waiter wakes via closeWithError.
        let result = await withTaskCancellationHandler(operation: { [self] () async -> Result<Void, WendyNetError> in
            var offset = 0
            let total = buffer.readableBytes
            while offset < total {
                if Task.isCancelled {
                    let currentError: WendyNetError? = state.withLockedValue { s in
                        if s.error == nil && !s.closed {
                            return nil  // need to close it
                        }
                        return s.error ?? .cancelled
                    }
                    if currentError == nil {
                        closeWithError(.cancelled)
                    }
                    let resolved: WendyNetError = state.withLockedValue { s in s.error ?? .cancelled }
                    return .failure(resolved)
                }
                let written = buffer.withUnsafeReadableBytes { raw -> Int32 in
                    guard let base = raw.baseAddress else { return -1 }
                    return WendyNetNative.send(
                        socketHandle: handle,
                        pointer: base + offset,
                        count: raw.count - offset
                    )
                }
                if written > 0 {
                    offset += Int(written)
                    continue
                }
                if written == 0 {
                    await waitForWritable()
                    let post: Result<Void, WendyNetError>? = state.withLockedValue { s in
                        if let err = s.error { return .failure(err) }
                        if s.closed { return .failure(.closed) }
                        return nil
                    }
                    if let post { return post }
                    continue
                }
                closeWithError(written == -2 ? .closed : .connectionFailed)
                let resolved: WendyNetError = state.withLockedValue { s in s.error ?? .closed }
                return .failure(resolved)
            }
            state.withLockedValue { s in s.lastActivity = WendyClock().now }
            return .success(())
        }, onCancel: { [self] in
            let shouldClose: Bool = state.withLockedValue { s in !s.closed && s.error == nil }
            if shouldClose {
                closeWithError(.cancelled)
            }
        })
        switch result {
        case .success: return
        case .failure(let err): throw err
        }
    }

    func executeThenClose<R: Sendable>(
        _ body: @Sendable (Inbound<Message>, Outbound<Message>) async throws(WendyNetError) -> R
    ) async throws(WendyNetError) -> R {
        // Atomic check-and-set: exactly one caller wins. A second concurrent
        // or sequential call throws `.alreadyConsumed`. Two concurrent calls
        // would race on the user's pipeline-stage state via the shared
        // decode/encode closures.
        let wasAlreadyUsed: Bool = state.withLockedValue { s in
            let prev = s.executeThenCloseUsed
            s.executeThenCloseUsed = true
            return prev
        }
        if wasAlreadyUsed {
            throw .alreadyConsumed
        }

        let inbound = Inbound<Message>(nextStep: { [self] in
            do throws(WendyNetError) {
                if let msg = try await receive() {
                    return .message(msg)
                }
                return .end
            } catch {
                return .failure(error)
            }
        })
        let outbound = Outbound<Message>(writeStep: { [self] msg in
            do throws(WendyNetError) {
                try await send(msg)
                return .accepted
            } catch {
                return .failure(error)
            }
        })

        // Run the body alongside the association's idle timer; cancelling the
        // group when the body returns tears the timer down with the channel.
        // If the timer fires first it closes the channel, surfacing
        // end-of-stream to the body.
        let outcome: Result<R, WendyNetError> =
            await withTaskGroup(of: Void.self, returning: Result<R, WendyNetError>.self) { group in
                if idleTimeout > .zero {
                    group.addTask { [self] in
                        await runIdleTimer(
                            remaining: { [self] in idleRemaining() },
                            sleep: { d in (try? await WendyClock().sleep(for: d)) != nil },
                            evict: { [self] in await close() }
                        )
                    }
                }
                let result: Result<R, WendyNetError>
                do throws(WendyNetError) {
                    result = .success(try await body(inbound, outbound))
                } catch {
                    result = .failure(error)
                }
                await close()
                group.cancelAll()
                return result
            }
        switch outcome {
        case .success(let v): return v
        case .failure(let e): throw e
        }
    }

    func drainReadable() {
        let alreadyDone: Bool = state.withLockedValue { s in s.closed || s.error != nil }
        if alreadyDone { return }
        // Sized to hold one whole UDP datagram (see wendyNetDatagramBufferSize).
        var buffer = [UInt8](repeating: 0, count: wendyNetDatagramBufferSize)

        while true {
            let read = WendyNetNative.recv(socketHandle: handle, into: &buffer)
            if read > 0 {
                let input = ByteBuffer(bytes: Array(buffer[0 ..< Int(read)]))
                let waiterToResume: (CheckedContinuation<Result<Message?, WendyNetError>, Never>, Result<Message?, WendyNetError>)? =
                    state.withLockedValue { s -> (CheckedContinuation<Result<Message?, WendyNetError>, Never>, Result<Message?, WendyNetError>)? in
                        s.lastActivity = WendyClock().now
                        s.decode(input, { message in
                            s.decodedMessages.append(message)
                        }, { failure in
                            s.error = failure
                        })
                        // If we picked up an error, fan it out to a receive waiter.
                        if let err = s.error, let waiter = s.receiveWaiter {
                            s.receiveWaiter = nil
                            return (waiter, .failure(err))
                        }
                        // If we have messages and someone is waiting, deliver.
                        if let waiter = s.receiveWaiter, !s.decodedMessages.isEmpty {
                            s.receiveWaiter = nil
                            return (waiter, .success(s.decodedMessages.removeFirst()))
                        }
                        return nil
                    }
                if let (waiter, result) = waiterToResume {
                    waiter.resume(returning: result)
                }

                let errored: Bool = state.withLockedValue { s in s.error != nil }
                if errored {
                    closeWithError(state.withLockedValue { s in s.error ?? .pipelineError })
                    return
                }
                let isClosed: Bool = state.withLockedValue { s in s.closed }
                if isClosed { return }
                continue
            }
            if read == 0 {
                let status = WendyNetNative.socketStatus(handle)
                if (status & wendyNetStatusClosed) != 0 {
                    closeCleanly()
                } else if (status & wendyNetStatusError) != 0 {
                    closeWithError(.connectionFailed)
                }
                return
            }
            if read == -2 {
                let status = WendyNetNative.socketStatus(handle)
                if (status & wendyNetStatusError) != 0 {
                    closeWithError(.connectionFailed)
                } else {
                    closeCleanly()
                }
            } else {
                closeWithError(.connectionFailed)
            }
            return
        }
    }

    func notifyWritableOrClosed() {
        let status = WendyNetNative.socketStatus(handle)
        if status < 0 {
            closeWithError(.connectionFailed)
            return
        }
        if (status & wendyNetStatusError) != 0 {
            closeWithError(.connectionFailed)
            return
        }
        if (status & wendyNetStatusClosed) != 0 {
            closeCleanly()
            return
        }
        if (status & wendyNetStatusWritable) != 0 {
            let waiter: CheckedContinuation<Void, Never>? = state.withLockedValue { s in
                let w = s.writableWaiter
                s.writableWaiter = nil
                return w
            }
            waiter?.resume()
        }
    }

    private func close() async {
        let firstClose: Bool = state.withLockedValue { s in
            if s.closed { return false }
            s.closed = true
            return true
        }
        if firstClose {
            WendyNetNative.closeSocket(handle)
            WendyNetState.shared.unregister(channelHandle: handle)
        }
        resumeWaitersOnClose()
    }

    // Lost-wakeup note (applies here and to `waitUntilWritable`): between
    // reading `socketStatus` and parking the continuation there is a window
    // in which the host can fire a writable event. On WASM's single-threaded
    // cooperative executor that's harmless -- the host's drain runs as a
    // `Task { ... }` enqueued by `networkEvent`, and cannot interleave with the
    // synchronous code that stores the continuation. On any pre-emptive or
    // multi-threaded executor the drain could acquire the state lock,
    // observe `writableWaiter == nil`, and exit; the waiter then parks
    // forever. Future ports to such an executor need a re-check after
    // parking (or an event-pending flag), not just the locks alone.
    private func waitForWritable() async {
        let status = WendyNetNative.socketStatus(handle)
        if (status & wendyNetStatusWritable) != 0 || (status & wendyNetStatusClosed) != 0 || (status & wendyNetStatusError) != 0 {
            notifyWritableOrClosed()
            return
        }
        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
            state.withLockedValue { s in s.writableWaiter = continuation }
        }
    }

    private func closeCleanly() {
        let firstClose: Bool = state.withLockedValue { s in
            if s.closed { return false }
            s.closed = true
            return true
        }
        if firstClose {
            WendyNetNative.closeSocket(handle)
            WendyNetState.shared.unregister(channelHandle: handle)
        }
        resumeWaitersOnClose()
    }

    private func closeWithError(_ newError: WendyNetError) {
        let firstClose: Bool = state.withLockedValue { s in
            if s.closed { return false }
            s.error = newError
            s.closed = true
            return true
        }
        if firstClose {
            WendyNetNative.closeSocket(handle)
            WendyNetState.shared.unregister(channelHandle: handle)
        }
        let waiters: (CheckedContinuation<Result<Message?, WendyNetError>, Never>?,
                      CheckedContinuation<Void, Never>?) =
            state.withLockedValue { s in
                let rw = s.receiveWaiter; s.receiveWaiter = nil
                let ww = s.writableWaiter; s.writableWaiter = nil
                return (rw, ww)
            }
        waiters.0?.resume(returning: .failure(newError))
        waiters.1?.resume()
    }

    private func resumeWaitersOnClose() {
        let waiters: (CheckedContinuation<Result<Message?, WendyNetError>, Never>?,
                      CheckedContinuation<Void, Never>?) =
            state.withLockedValue { s in
                let rw = s.receiveWaiter; s.receiveWaiter = nil
                let ww = s.writableWaiter; s.writableWaiter = nil
                return (rw, ww)
            }
        waiters.0?.resume(returning: .success(nil))
        waiters.1?.resume()
    }

    // See the lost-wakeup note on `waitForWritable`. The `while true` here
    // re-reads status after each wake, but that only helps if *some* future
    // event resumes the continuation. On a multi-threaded executor where
    // the drain races with parking, no further event may fire and this loop
    // hangs.
    func waitUntilWritable() async throws(WendyNetError) {
        while true {
            let status = WendyNetNative.socketStatus(handle)
            if status < 0 {
                closeWithError(.connectionFailed)
                throw .connectionFailed
            }
            if (status & wendyNetStatusError) != 0 {
                closeWithError(.connectionFailed)
                throw .connectionFailed
            }
            if (status & wendyNetStatusClosed) != 0 {
                closeCleanly()
                throw .closed
            }
            if (status & wendyNetStatusWritable) != 0 {
                return
            }

            await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
                state.withLockedValue { s in s.writableWaiter = continuation }
            }
        }
    }
}

// MARK: - Bootstrap entry points

extension ClientBootstrap {
    /// Connect to an endpoint. TAPS-style transport racing happens internally.
    public func connect(to endpoint: Endpoint) async throws(WendyNetError) -> Channel<Message> {
        let _ = security
        guard WendyNetState.shared.ensureInitialized() else {
            throw .connectionFailed
        }

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
        let handle: Int32
        if isStream {
            handle = WendyNetNative.connect(hostname: hostname, port: port)
        } else {
            handle = WendyNetNative.udpConnect(hostname: hostname, port: port)
        }
        if handle <= 0 {
            throw .connectionFailed
        }

        let transport = TransportInfo(kind: isStream ? .tcp : .udp, isStream: isStream)
        var closures = pipelineFactory()
        // Framers are only meaningful on stream transports; datagrams already
        // carry message boundaries.
        if let framerFactory = framerFactory, isStream {
            closures = framerFactory().composing(closures)
        }

        let context = ConnectionContext(remoteEndpoint: endpoint, transport: transport, security: security)

        let core = ChannelCore<Message>(
            handle: handle,
            endpoint: endpoint,
            transport: transport,
            decode: closures.decode,
            encode: closures.encode
        )
        WendyNetState.shared.register(channel: core)

        do throws(WendyNetError) {
            try await core.waitUntilWritable()
        } catch {
            WendyNetState.shared.unregister(channelHandle: handle)
            WendyNetNative.closeSocket(handle)
            throw error
        }

        closures.started(context)

        return Channel(
            endpoint: endpoint,
            transport: transport,
            maximumMessageLength: wendyNetMaximumMessageLength,
            core: core
        )
    }
}

extension ServerBootstrap {
    /// Bind to a port.
    public func bind(port: UInt16) async throws(WendyNetError) -> Listener<Message> {
        let _ = wendyNet
        guard WendyNetState.shared.ensureInitialized() else {
            throw .listenerError
        }

        let isStream = reliability == .reliable
        let transport = TransportInfo(kind: isStream ? .tcp : .udp, isStream: isStream)
        let endpoint = Endpoint.ipHost(hostname: "0.0.0.0", port: port)
        let context = ConnectionContext(remoteEndpoint: endpoint, transport: transport, security: security)

        let handle: Int32
        if isStream {
            handle = WendyNetNative.listen(port: port, backlog: 4)
        } else {
            handle = WendyNetNative.udpListen(port: port)
        }
        if handle <= 0 {
            throw .listenerError
        }

        // Read back the actual bound port. If the caller passed 0 the OS
        // assigned an ephemeral port; either way the resolved value is what
        // consumers want exposed on the Listener.
        let resolvedRaw = WendyNetNative.listenerPort(handle)
        if resolvedRaw < 0 {
            WendyNetNative.closeListener(handle)
            throw .listenerError
        }
        let resolvedPort = UInt16(resolvedRaw)

        let core = ListenerCore<Message>(
            handle: handle,
            port: resolvedPort,
            context: context,
            udpAssociationTimeout: isStream ? .zero : udpAssociationTimeout,
            isStream: isStream,
            pipelineFactory: pipelineFactory,
            framerFactory: framerFactory
        )
        WendyNetState.shared.register(listener: core)
        return Listener(port: resolvedPort, core: core)
    }
}

#endif
