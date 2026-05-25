#if WendyNetBackendWendyLite
import CWendyNet
import WendyLite
import Synchronization
import _Concurrency

// MARK: - Internal lock primitive
//
// Module-local copy of the spinlock-backed lock-box used in the WendyLite
// library. The canonical rationale lives in `wendy-lite/Sources/WendyLite/
// Internal.swift` -- this file's `_LockedBox` has the same shape, semantics,
// and safety story (atomic-spinlock, `@unchecked Sendable`, accepts
// non-`Sendable` `T` so it can wrap user pipeline-stage closures the same
// way SwiftNIO confines `ChannelHandler`s to an `EventLoop`). The
// duplication exists because `_LockedBox` in WendyLite is not part of its
// public API and we don't want to expose it.

fileprivate final class _LockedBox<T>: @unchecked Sendable {
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

    static func connect(hostname: String, port: UInt16) -> Int32 {
        let bytes = Array(hostname.utf8)
        return bytes.withUnsafeBufferPointer { ptr in
            guard let baseAddress = ptr.baseAddress else { return -1 }
            return wendynet_tcp_connect(
                UnsafeRawPointer(baseAddress).assumingMemoryBound(to: CChar.self),
                Int32(ptr.count),
                Int32(port)
            )
        }
    }

    static func accept(listenerHandle: Int32) -> Int32 {
        wendynet_listener_accept(listenerHandle)
    }

    @discardableResult
    static func closeListener(_ handle: Int32) -> Int32 {
        wendynet_listener_close(handle)
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
    var isOpen: Bool { get }
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
    private let state = _LockedBox(State())

    func ensureInitialized() -> Bool {
        let alreadyInit = state.withLockedValue { s in s.initialized }
        if alreadyInit { return true }
        registerWendyNetCallback()
        let ok = WendyNetNative.initialize() == 0
        state.withLockedValue { s in s.initialized = ok }
        return ok
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

    func drainNativeEvents() {
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
    private struct State {
        var pendingChannels: [Channel<Message>] = []
        var acceptWaiter: CheckedContinuation<Result<Channel<Message>?, WendyNetError>, Never>? = nil
        var isClosed = false
        var closures: _PipelineClosures<Message>
        /// Single-use latch -- see `executeThenClose` for rationale.
        var executeThenCloseUsed = false
    }
    private let state: _LockedBox<State>

    init(handle: Int32, port: UInt16, context: ConnectionContext, closures: _PipelineClosures<Message>) {
        self.handle = handle
        self.port = port
        self.context = context
        self.state = _LockedBox(State(closures: closures))
    }

    func accept() async throws(WendyNetError) -> Channel<Message>? {
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
        let fast2: Result<Channel<Message>?, WendyNetError>? = state.withLockedValue { s in
            if !s.pendingChannels.isEmpty { return .success(s.pendingChannels.removeFirst()) }
            if s.isClosed { return .success(nil) }
            return nil
        }
        if let fast2 {
            switch fast2 {
            case .success(let c): return c
            case .failure(let e): throw e
            }
        }

        let result = await withTaskCancellationHandler(operation: { [self] () async -> Result<Channel<Message>?, WendyNetError> in
            await withCheckedContinuation { (continuation: CheckedContinuation<Result<Channel<Message>?, WendyNetError>, Never>) in
                let resume: Result<Channel<Message>?, WendyNetError>? = state.withLockedValue { s in
                    if Task.isCancelled { return .failure(.cancelled) }
                    if !s.pendingChannels.isEmpty { return .success(s.pendingChannels.removeFirst()) }
                    if s.isClosed { return .success(nil) }
                    if s.acceptWaiter != nil {
                        return .failure(.concurrentAccess)
                    }
                    s.acceptWaiter = continuation
                    return nil
                }
                if let resume {
                    continuation.resume(returning: resume)
                }
            }
        }, onCancel: { [self] in
            let waiter: CheckedContinuation<Result<Channel<Message>?, WendyNetError>, Never>? =
                state.withLockedValue { s in
                    let w = s.acceptWaiter
                    s.acceptWaiter = nil
                    return w
                }
            waiter?.resume(returning: .failure(.cancelled))
        })
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

        let accepted = Accepted<Message>(_next: { [self] in
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
                // Build a fresh channel core under our lock so the per-connection
                // closures are constructed once and stay confined to that core.
                let (channelCore, channel): (ChannelCore<Message>, Channel<Message>) =
                    state.withLockedValue { s in
                        let core = ChannelCore<Message>(
                            handle: socketHandle,
                            endpoint: context.remoteEndpoint,
                            transport: context.transport,
                            decode: s.closures.decode,
                            encode: s.closures.encode
                        )
                        let channel = Channel<Message>(
                            endpoint: context.remoteEndpoint,
                            transport: context.transport,
                            maximumMessageLength: wendyNetMaximumMessageLength,
                            core: core
                        )
                        return (core, channel)
                    }
                WendyNetState.shared.register(channel: channelCore)

                let resume: CheckedContinuation<Result<Channel<Message>?, WendyNetError>, Never>? =
                    state.withLockedValue { s in
                        if let waiter = s.acceptWaiter {
                            s.acceptWaiter = nil
                            return waiter
                        }
                        s.pendingChannels.append(channel)
                        return nil
                    }
                resume?.resume(returning: .success(channel))
                continue
            }
            if socketHandle < 0 {
                closeWithError(.listenerError)
            }
            break
        }
    }

    func close() async {
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
    private struct State {
        var decode: (ByteBuffer, (Message) -> Void, (WendyNetError) -> Void) -> Void
        var encode: (Message) -> ByteBuffer
        var decodedMessages: [Message] = []
        var receiveWaiter: CheckedContinuation<Result<Message?, WendyNetError>, Never>? = nil
        var writableWaiter: CheckedContinuation<Void, Never>? = nil
        var closed = false
        var error: WendyNetError? = nil
        /// Single-use latch -- see `executeThenClose` for rationale.
        var executeThenCloseUsed = false
    }
    private let state: _LockedBox<State>

    var isOpen: Bool {
        state.withLockedValue { s in !s.closed && s.error == nil }
    }

    init(
        handle: Int32,
        endpoint: Endpoint,
        transport: TransportInfo,
        decode: @escaping (ByteBuffer, (Message) -> Void, (WendyNetError) -> Void) -> Void,
        encode: @escaping (Message) -> ByteBuffer
    ) {
        self.handle = handle
        self.endpoint = endpoint
        self.transport = transport
        self.state = _LockedBox(State(decode: decode, encode: encode))
    }

    func receive() async throws(WendyNetError) -> Message? {
        // Fast paths
        if let early = tryDeliverReceived() {
            switch early {
            case .success(let m): return m
            case .failure(let e): throw e
            }
        }
        drainReadable()
        if let early = tryDeliverReceived() {
            switch early {
            case .success(let m): return m
            case .failure(let e): throw e
            }
        }

        let result = await withTaskCancellationHandler(operation: { [self] () async -> Result<Message?, WendyNetError> in
            await withCheckedContinuation { (continuation: CheckedContinuation<Result<Message?, WendyNetError>, Never>) in
                let resumeNow: Result<Message?, WendyNetError>? = state.withLockedValue { s in
                    if Task.isCancelled { return .failure(.cancelled) }
                    if !s.decodedMessages.isEmpty { return .success(s.decodedMessages.removeFirst()) }
                    if let err = s.error { return .failure(err) }
                    if s.closed { return .success(nil) }
                    if s.receiveWaiter != nil {
                        return .failure(.concurrentAccess)
                    }
                    s.receiveWaiter = continuation
                    return nil
                }
                if let resumeNow {
                    continuation.resume(returning: resumeNow)
                }
            }
        }, onCancel: { [self] in
            let waiter: CheckedContinuation<Result<Message?, WendyNetError>, Never>? =
                state.withLockedValue { s in
                    let w = s.receiveWaiter
                    s.receiveWaiter = nil
                    return w
                }
            waiter?.resume(returning: .failure(.cancelled))
        })
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

    func send(_ message: Message) async throws(WendyNetError) -> SendResult {
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
        let result = await withTaskCancellationHandler(operation: { [self] () async -> Result<SendResult, WendyNetError> in
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
                    let post: Result<SendResult, WendyNetError>? = state.withLockedValue { s in
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
            return .success(.accepted)
        }, onCancel: { [self] in
            let shouldClose: Bool = state.withLockedValue { s in !s.closed && s.error == nil }
            if shouldClose {
                closeWithError(.cancelled)
            }
        })
        switch result {
        case .success(let r): return r
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

        let inbound = Inbound<Message>(_next: { [self] in
            do throws(WendyNetError) {
                if let msg = try await receive() {
                    return .message(msg)
                }
                return .end
            } catch {
                return .failure(error)
            }
        })
        let outbound = Outbound<Message>(_write: { [self] msg in
            do throws(WendyNetError) {
                let result = try await send(msg)
                return .accepted(result)
            } catch {
                return .failure(error)
            }
        })
        let outcome: Result<R, WendyNetError>
        do throws(WendyNetError) {
            let value = try await body(inbound, outbound)
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

    func drainReadable() {
        let alreadyDone: Bool = state.withLockedValue { s in s.closed || s.error != nil }
        if alreadyDone { return }
        var buffer = [UInt8](repeating: 0, count: 256)

        while true {
            let read = WendyNetNative.recv(socketHandle: handle, into: &buffer)
            if read > 0 {
                let input = ByteBuffer(bytes: Array(buffer[0 ..< Int(read)]))
                let waiterToResume: (CheckedContinuation<Result<Message?, WendyNetError>, Never>, Result<Message?, WendyNetError>)? =
                    state.withLockedValue { s -> (CheckedContinuation<Result<Message?, WendyNetError>, Never>, Result<Message?, WendyNetError>)? in
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

    func close() async {
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
        let _ = udpAssociationTimeoutSeconds
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

        let handle = WendyNetNative.connect(hostname: hostname, port: port)
        if handle <= 0 {
            throw .connectionFailed
        }

        let transport = TransportInfo(kind: .tcp, isStream: true)
        var closures = _pipelineFactory()
        if let framerFactory = _framerFactory {
            closures = framerFactory().composing(closures)
        }

        let context = ConnectionContext(remoteEndpoint: endpoint, transport: transport, security: security)
        closures.started(context)

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
        let _ = udpAssociationTimeoutSeconds
        guard WendyNetState.shared.ensureInitialized() else {
            throw .listenerError
        }

        let transport = TransportInfo(kind: .tcp, isStream: true)
        let endpoint = Endpoint.ipHost(hostname: "0.0.0.0", port: port)
        var closures = _pipelineFactory()
        if let framerFactory = _framerFactory {
            closures = framerFactory().composing(closures)
        }

        let context = ConnectionContext(remoteEndpoint: endpoint, transport: transport, security: security)
        closures.started(context)

        let handle = WendyNetNative.listen(port: port, backlog: 4)
        if handle <= 0 {
            throw .listenerError
        }

        let core = ListenerCore<Message>(handle: handle, port: port, context: context, closures: closures)
        WendyNetState.shared.register(listener: core)
        return Listener(port: port, core: core)
    }
}

#endif
