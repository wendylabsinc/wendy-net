#if WendyNetBackendWendyLite
import CWendyNet
import WendyLite
import _Concurrency

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

    static func send(socketHandle: Int32, bytes: [UInt8], offset: Int) -> Int32 {
        bytes.withUnsafeBufferPointer { ptr in
            guard let baseAddress = ptr.baseAddress else { return -1 }
            return wendynet_socket_send(
                socketHandle,
                UnsafeRawPointer(baseAddress + offset).assumingMemoryBound(to: CChar.self),
                Int32(bytes.count - offset)
            )
        }
    }

    @discardableResult
    static func closeSocket(_ handle: Int32) -> Int32 {
        wendynet_socket_close(handle)
    }
}

// MARK: - Runtime State

fileprivate protocol AnyListenerCore: AnyObject {
    var handle: Int32 { get }
    func drainAccepted()
    func closeFromReset()
}

fileprivate protocol AnyChannelCore: AnyObject {
    var handle: Int32 { get }
    var isOpen: Bool { get }
    func drainReadable()
    func notifyWritableOrClosed()
    func closeFromReset()
}

fileprivate enum WendyNetState {
    nonisolated(unsafe) static var shared = WendyNetHub()
}

fileprivate final class WendyNetHub: @unchecked Sendable {
    private var listeners: [AnyListenerCore] = []
    private var channels: [AnyChannelCore] = []
    private var isDraining = false
    private var initialized = false

    func ensureInitialized() -> Bool {
        if initialized { return true }
        registerWendyNetCallback()
        let ok = WendyNetNative.initialize() == 0
        initialized = ok
        return ok
    }

    func register(listener: AnyListenerCore) {
        listeners.append(listener)
    }

    func unregister(listenerHandle: Int32) {
        listeners.removeAll { $0.handle == listenerHandle }
    }

    func register(channel: AnyChannelCore) {
        channels.append(channel)
    }

    func unregister(channelHandle: Int32) {
        channels.removeAll { $0.handle == channelHandle }
    }

    func networkEvent(bits: Int32) {
        let _ = bits
        Task { [self] in
            drainNativeEvents()
        }
    }

    func drainNativeEvents() {
        if isDraining {
            return
        }
        isDraining = true
        defer { isDraining = false }

        while true {
            let bits = WendyNetNative.drainEvents()
            if bits == 0 {
                break
            }

            if (bits & wendyNetEventAcceptReady) != 0 {
                for listener in listeners {
                    listener.drainAccepted()
                }
            }

            if (bits & (wendyNetEventReadReady | wendyNetEventClosed | wendyNetEventError)) != 0 {
                for channel in channels {
                    channel.drainReadable()
                }
            }

            if (bits & (wendyNetEventWriteReady | wendyNetEventClosed | wendyNetEventError)) != 0 {
                for channel in channels {
                    channel.notifyWritableOrClosed()
                }
            }
        }
    }
}

// MARK: - ListenerCore

final class ListenerCore<Message: Sendable>: AnyListenerCore, @unchecked Sendable {
    let handle: Int32
    let port: UInt16
    let context: ConnectionContext
    let closures: _PipelineClosures<Message>
    private var pendingChannels: [Channel<Message>] = []
    private var acceptWaiter: CheckedContinuation<Result<Channel<Message>?, WendyNetError>, Never>?
    private var isClosed = false

    init(handle: Int32, port: UInt16, context: ConnectionContext, closures: _PipelineClosures<Message>) {
        self.handle = handle
        self.port = port
        self.context = context
        self.closures = closures
    }

    func accept() async throws(WendyNetError) -> Channel<Message>? {
        if !pendingChannels.isEmpty {
            return pendingChannels.removeFirst()
        }
        if isClosed {
            return nil
        }

        drainAccepted()
        if !pendingChannels.isEmpty {
            return pendingChannels.removeFirst()
        }

        let result = await withCheckedContinuation { continuation in
            acceptWaiter = continuation
        }
        switch result {
        case .success(let channel):
            return channel
        case .failure(let error):
            throw error
        }
    }

    func drainAccepted() {
        guard !isClosed else { return }
        while true {
            let socketHandle = WendyNetNative.accept(listenerHandle: handle)
            if socketHandle > 0 {
                let channelCore = ChannelCore<Message>(
                    handle: socketHandle,
                    endpoint: context.remoteEndpoint,
                    transport: context.transport,
                    decode: closures.decode,
                    encode: closures.encode
                )
                let channel = Channel<Message>(
                    endpoint: context.remoteEndpoint,
                    transport: context.transport,
                    maximumMessageLength: wendyNetMaximumMessageLength,
                    core: channelCore
                )
                WendyNetState.shared.register(channel: channelCore)

                if let waiter = acceptWaiter {
                    acceptWaiter = nil
                    waiter.resume(returning: .success(channel))
                } else {
                    pendingChannels.append(channel)
                }
                continue
            }
            if socketHandle < 0 {
                closeWithError(.listenerError)
            }
            break
        }
    }

    func close() async {
        if isClosed {
            return
        }
        isClosed = true
        WendyNetNative.closeListener(handle)
        WendyNetState.shared.unregister(listenerHandle: handle)
        if let waiter = acceptWaiter {
            acceptWaiter = nil
            waiter.resume(returning: .success(nil))
        }
    }

    func closeFromReset() {
        isClosed = true
        if let waiter = acceptWaiter {
            acceptWaiter = nil
            waiter.resume(returning: .success(nil))
        }
    }

    private func closeWithError(_ error: WendyNetError) {
        isClosed = true
        WendyNetState.shared.unregister(listenerHandle: handle)
        if let waiter = acceptWaiter {
            acceptWaiter = nil
            waiter.resume(returning: .failure(error))
        }
    }
}

// MARK: - ChannelCore

final class ChannelCore<Message: Sendable>: AnyChannelCore, @unchecked Sendable {
    let handle: Int32
    let endpoint: Endpoint
    let transport: TransportInfo
    private let decode: @Sendable (ByteBuffer, (Message) -> Void, (WendyNetError) -> Void) -> Void
    private let encode: @Sendable (Message) -> ByteBuffer
    private var decodedMessages: [Message] = []
    private var receiveWaiter: CheckedContinuation<Result<Message?, WendyNetError>, Never>?
    private var writableWaiter: CheckedContinuation<Void, Never>?
    private var closed = false
    private var error: WendyNetError?

    var isOpen: Bool { !closed && error == nil }

    init(
        handle: Int32,
        endpoint: Endpoint,
        transport: TransportInfo,
        decode: @escaping @Sendable (ByteBuffer, (Message) -> Void, (WendyNetError) -> Void) -> Void,
        encode: @escaping @Sendable (Message) -> ByteBuffer
    ) {
        self.handle = handle
        self.endpoint = endpoint
        self.transport = transport
        self.decode = decode
        self.encode = encode
    }

    func receive() async throws(WendyNetError) -> Message? {
        if !decodedMessages.isEmpty {
            return decodedMessages.removeFirst()
        }
        if let error {
            throw error
        }
        if closed {
            return nil
        }

        drainReadable()
        if !decodedMessages.isEmpty {
            return decodedMessages.removeFirst()
        }
        if let error {
            throw error
        }
        if closed {
            return nil
        }

        let result = await withCheckedContinuation { continuation in
            receiveWaiter = continuation
        }
        switch result {
        case .success(let message):
            return message
        case .failure(let error):
            throw error
        }
    }

    func send(_ message: Message) async throws(WendyNetError) -> SendResult {
        if let error {
            throw error
        }
        if closed {
            throw .closed
        }

        let bytes = encode(message).readableBytesArray()
        var offset = 0
        while offset < bytes.count {
            let written = WendyNetNative.send(socketHandle: handle, bytes: bytes, offset: offset)
            if written > 0 {
                offset += Int(written)
                continue
            }
            if written == 0 {
                await waitForWritable()
                if let error {
                    throw error
                }
                if closed {
                    throw .closed
                }
                continue
            }
            closeWithError(written == -2 ? .closed : .connectionFailed)
            throw error ?? .closed
        }
        return .accepted
    }

    func drainReadable() {
        guard !closed, error == nil else { return }
        var buffer = [UInt8](repeating: 0, count: 256)

        while true {
            let read = WendyNetNative.recv(socketHandle: handle, into: &buffer)
            if read > 0 {
                let input = ByteBuffer(bytes: Array(buffer[0 ..< Int(read)]))
                decode(input, { message in
                    decodedMessages.append(message)
                }, { failure in
                    closeWithError(failure)
                })
                resumeReceiveIfPossible()
                if error != nil || closed {
                    return
                }
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
        if (status & wendyNetStatusWritable) != 0, let waiter = writableWaiter {
            writableWaiter = nil
            waiter.resume()
        }
    }

    func close() async {
        if closed {
            return
        }
        closed = true
        WendyNetNative.closeSocket(handle)
        WendyNetState.shared.unregister(channelHandle: handle)
        resumeWaitersOnClose()
    }

    func closeFromReset() {
        closed = true
        resumeWaitersOnClose()
    }

    private func waitForWritable() async {
        let status = WendyNetNative.socketStatus(handle)
        if (status & wendyNetStatusWritable) != 0 || (status & wendyNetStatusClosed) != 0 || (status & wendyNetStatusError) != 0 {
            notifyWritableOrClosed()
            return
        }
        await withCheckedContinuation { continuation in
            writableWaiter = continuation
        }
    }

    private func resumeReceiveIfPossible() {
        guard let waiter = receiveWaiter, !decodedMessages.isEmpty else { return }
        receiveWaiter = nil
        waiter.resume(returning: .success(decodedMessages.removeFirst()))
    }

    private func closeCleanly() {
        closed = true
        WendyNetNative.closeSocket(handle)
        WendyNetState.shared.unregister(channelHandle: handle)
        resumeWaitersOnClose()
    }

    private func closeWithError(_ newError: WendyNetError) {
        error = newError
        closed = true
        WendyNetNative.closeSocket(handle)
        WendyNetState.shared.unregister(channelHandle: handle)
        if let waiter = receiveWaiter {
            receiveWaiter = nil
            waiter.resume(returning: .failure(newError))
        }
        if let waiter = writableWaiter {
            writableWaiter = nil
            waiter.resume()
        }
    }

    private func resumeWaitersOnClose() {
        if let waiter = receiveWaiter {
            receiveWaiter = nil
            waiter.resume(returning: .success(nil))
        }
        if let waiter = writableWaiter {
            writableWaiter = nil
            waiter.resume()
        }
    }

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

            await withCheckedContinuation { continuation in
                writableWaiter = continuation
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
