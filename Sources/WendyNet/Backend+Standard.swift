#if WendyNetBackendStandard
import NIOCore
import NIOPosix
import NIOConcurrencyHelpers
import _Concurrency

private let standardEventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)

private typealias NIOByteBuffer = NIOCore.ByteBuffer

// MARK: - Inbound NIO handler

private final class WendyNetInboundHandler<Message: Sendable>: ChannelInboundHandler, @unchecked Sendable {
    typealias InboundIn = NIOByteBuffer

    private let core: ChannelCore<Message>

    init(core: ChannelCore<Message>) {
        self.core = core
    }

    func channelActive(context: ChannelHandlerContext) {
        core.attach(nioChannel: context.channel)
        context.fireChannelActive()
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var nioBuf = unwrapInboundIn(data)
        let bytes = nioBuf.readBytes(length: nioBuf.readableBytes) ?? []
        core.deliverInbound(ByteBuffer(bytes: bytes))
    }

    func channelInactive(context: ChannelHandlerContext) {
        core.handleRemoteClose()
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        core.handleError(.connectionFailed)
        context.fireErrorCaught(error)
    }
}

// MARK: - ChannelCore

final class ChannelCore<Message: Sendable>: @unchecked Sendable {
    let endpoint: Endpoint
    let transport: TransportInfo
    private let decode: @Sendable (ByteBuffer, (Message) -> Void, (WendyNetError) -> Void) -> Void
    private let encode: @Sendable (Message) -> ByteBuffer

    private struct State {
        var nioChannel: NIOCore.Channel?
        var decodedMessages: [Message] = []
        var receiveWaiter: CheckedContinuation<Result<Message?, WendyNetError>, Never>?
        var attachWaiter: CheckedContinuation<Void, Never>?
        var closed = false
        var error: WendyNetError?
    }
    private let state = NIOLockedValueBox(State())

    var isOpen: Bool {
        state.withLockedValue { !$0.closed && $0.error == nil }
    }

    init(
        endpoint: Endpoint,
        transport: TransportInfo,
        decode: @escaping @Sendable (ByteBuffer, (Message) -> Void, (WendyNetError) -> Void) -> Void,
        encode: @escaping @Sendable (Message) -> ByteBuffer
    ) {
        self.endpoint = endpoint
        self.transport = transport
        self.decode = decode
        self.encode = encode
    }

    func attach(nioChannel: NIOCore.Channel) {
        let waiter: CheckedContinuation<Void, Never>? = state.withLockedValue { s in
            s.nioChannel = nioChannel
            let waiter = s.attachWaiter
            s.attachWaiter = nil
            return waiter
        }
        waiter?.resume()
    }

    private func awaitAttach() async {
        let needsWait: Bool = state.withLockedValue { s in s.nioChannel == nil && !s.closed && s.error == nil }
        guard needsWait else { return }
        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
            let resumeNow: Bool = state.withLockedValue { s in
                if s.nioChannel != nil || s.closed || s.error != nil {
                    return true
                }
                s.attachWaiter = continuation
                return false
            }
            if resumeNow { continuation.resume() }
        }
    }

    func deliverInbound(_ buf: ByteBuffer) {
        var produced: [Message] = []
        var failure: WendyNetError?
        decode(buf, { msg in
            produced.append(msg)
        }, { err in
            failure = err
        })

        if let failure {
            handleError(failure)
            return
        }

        let waiter: CheckedContinuation<Result<Message?, WendyNetError>, Never>?
        let firstMessage: Message?
        (waiter, firstMessage) = state.withLockedValue { s in
            if produced.isEmpty {
                return (nil, nil)
            }
            if let waiter = s.receiveWaiter {
                s.receiveWaiter = nil
                let head = produced.removeFirst()
                s.decodedMessages.append(contentsOf: produced)
                return (waiter, head)
            } else {
                s.decodedMessages.append(contentsOf: produced)
                return (nil, nil)
            }
        }
        if let waiter, let firstMessage {
            waiter.resume(returning: .success(firstMessage))
        }
    }

    func handleRemoteClose() {
        let waiter: CheckedContinuation<Result<Message?, WendyNetError>, Never>? = state.withLockedValue { s in
            s.closed = true
            let w = s.receiveWaiter
            s.receiveWaiter = nil
            return w
        }
        waiter?.resume(returning: .success(nil))
    }

    func handleError(_ err: WendyNetError) {
        let waiter: CheckedContinuation<Result<Message?, WendyNetError>, Never>? = state.withLockedValue { s in
            if s.error == nil { s.error = err }
            s.closed = true
            let w = s.receiveWaiter
            s.receiveWaiter = nil
            return w
        }
        waiter?.resume(returning: .failure(err))
    }

    func receive() async throws(WendyNetError) -> Message? {
        let snapshot: Result<Message?, WendyNetError>?
        snapshot = state.withLockedValue { s in
            if !s.decodedMessages.isEmpty {
                return .success(s.decodedMessages.removeFirst())
            }
            if let err = s.error {
                return .failure(err)
            }
            if s.closed {
                return .success(nil)
            }
            return nil
        }
        if let snapshot {
            switch snapshot {
            case .success(let msg): return msg
            case .failure(let err): throw err
            }
        }

        let result = await withCheckedContinuation { (continuation: CheckedContinuation<Result<Message?, WendyNetError>, Never>) in
            let resumeNow: Result<Message?, WendyNetError>? = state.withLockedValue { s in
                if !s.decodedMessages.isEmpty {
                    return .success(s.decodedMessages.removeFirst())
                }
                if let err = s.error {
                    return .failure(err)
                }
                if s.closed {
                    return .success(nil)
                }
                s.receiveWaiter = continuation
                return nil
            }
            if let resumeNow {
                continuation.resume(returning: resumeNow)
            }
        }
        switch result {
        case .success(let msg): return msg
        case .failure(let err): throw err
        }
    }

    func send(_ message: Message) async throws(WendyNetError) -> SendResult {
        await awaitAttach()
        let snapshot: (channel: NIOCore.Channel?, error: WendyNetError?, closed: Bool) = state.withLockedValue { s in
            (s.nioChannel, s.error, s.closed)
        }
        if let err = snapshot.error { throw err }
        if snapshot.closed { throw .closed }
        guard let nioChannel = snapshot.channel else { throw .connectionFailed }

        let bytes = encode(message).readableBytesArray()
        var nioBuf = nioChannel.allocator.buffer(capacity: bytes.count)
        nioBuf.writeBytes(bytes)
        do {
            try await nioChannel.writeAndFlush(nioBuf)
        } catch {
            handleError(.connectionFailed)
            throw .connectionFailed
        }
        return .accepted
    }

    func close() async {
        let nioChannel: NIOCore.Channel? = state.withLockedValue { s in
            if s.closed { return nil }
            s.closed = true
            return s.nioChannel
        }
        if let nioChannel {
            try? await nioChannel.close().get()
        }
        let waiter: CheckedContinuation<Result<Message?, WendyNetError>, Never>? = state.withLockedValue { s in
            let w = s.receiveWaiter
            s.receiveWaiter = nil
            return w
        }
        waiter?.resume(returning: .success(nil))
    }
}

// MARK: - ListenerCore

final class ListenerCore<Message: Sendable>: @unchecked Sendable {
    let port: UInt16
    let context: ConnectionContext
    let closures: _PipelineClosures<Message>

    private struct State {
        var nioChannel: NIOCore.Channel?
        var pendingChannels: [Channel<Message>] = []
        var acceptWaiter: CheckedContinuation<Result<Channel<Message>?, WendyNetError>, Never>?
        var closed = false
    }
    private let state = NIOLockedValueBox(State())

    init(port: UInt16, context: ConnectionContext, closures: _PipelineClosures<Message>) {
        self.port = port
        self.context = context
        self.closures = closures
    }

    func attach(nioChannel: NIOCore.Channel) {
        state.withLockedValue { s in s.nioChannel = nioChannel }
    }

    func enqueueAccepted(_ channel: Channel<Message>) {
        let waiter: CheckedContinuation<Result<Channel<Message>?, WendyNetError>, Never>? = state.withLockedValue { s in
            if s.closed { return nil }
            if let waiter = s.acceptWaiter {
                s.acceptWaiter = nil
                return waiter
            }
            s.pendingChannels.append(channel)
            return nil
        }
        waiter?.resume(returning: .success(channel))
    }

    func accept() async throws(WendyNetError) -> Channel<Message>? {
        let snapshot: Result<Channel<Message>?, WendyNetError>? = state.withLockedValue { s in
            if !s.pendingChannels.isEmpty {
                return .success(s.pendingChannels.removeFirst())
            }
            if s.closed {
                return .success(nil)
            }
            return nil
        }
        if let snapshot {
            switch snapshot {
            case .success(let channel): return channel
            case .failure(let err): throw err
            }
        }

        let result = await withCheckedContinuation { (continuation: CheckedContinuation<Result<Channel<Message>?, WendyNetError>, Never>) in
            let resumeNow: Result<Channel<Message>?, WendyNetError>? = state.withLockedValue { s in
                if !s.pendingChannels.isEmpty {
                    return .success(s.pendingChannels.removeFirst())
                }
                if s.closed {
                    return .success(nil)
                }
                s.acceptWaiter = continuation
                return nil
            }
            if let resumeNow {
                continuation.resume(returning: resumeNow)
            }
        }
        switch result {
        case .success(let channel): return channel
        case .failure(let err): throw err
        }
    }

    func close() async {
        let (nioChannel, waiter): (NIOCore.Channel?, CheckedContinuation<Result<Channel<Message>?, WendyNetError>, Never>?) = state.withLockedValue { s in
            if s.closed { return (nil, nil) }
            s.closed = true
            let w = s.acceptWaiter
            s.acceptWaiter = nil
            return (s.nioChannel, w)
        }
        if let nioChannel {
            try? await nioChannel.close().get()
        }
        waiter?.resume(returning: .success(nil))
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

        let core = ChannelCore<Message>(
            endpoint: endpoint,
            transport: transport,
            decode: closures.decode,
            encode: closures.encode
        )

        let nioBootstrap = NIOPosix.ClientBootstrap(group: standardEventLoopGroup)
            .channelInitializer { nioChannel in
                nioChannel.pipeline.addHandler(WendyNetInboundHandler<Message>(core: core))
            }

        do {
            _ = try await nioBootstrap.connect(host: hostname, port: Int(port)).get()
        } catch {
            throw .connectionFailed
        }

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
        var closures = _pipelineFactory()
        if let framerFactory = _framerFactory {
            closures = framerFactory().composing(closures)
        }

        let context = ConnectionContext(remoteEndpoint: endpoint, transport: transport, security: security)
        closures.started(context)

        let listenerCore = ListenerCore<Message>(port: port, context: context, closures: closures)

        let frozenClosures = closures
        let frozenContext = context
        let nioBootstrap = NIOPosix.ServerBootstrap(group: standardEventLoopGroup)
            .serverChannelOption(ChannelOptions.backlog, value: 4)
            .childChannelInitializer { nioChildChannel in
                let childCore = ChannelCore<Message>(
                    endpoint: frozenContext.remoteEndpoint,
                    transport: frozenContext.transport,
                    decode: frozenClosures.decode,
                    encode: frozenClosures.encode
                )
                let acceptedChannel = Channel<Message>(
                    endpoint: frozenContext.remoteEndpoint,
                    transport: frozenContext.transport,
                    maximumMessageLength: wendyNetMaximumMessageLength,
                    core: childCore
                )
                listenerCore.enqueueAccepted(acceptedChannel)
                return nioChildChannel.pipeline.addHandler(WendyNetInboundHandler<Message>(core: childCore))
            }

        let serverChannel: NIOCore.Channel
        do {
            serverChannel = try await nioBootstrap.bind(host: "0.0.0.0", port: Int(port)).get()
        } catch {
            throw .listenerError
        }
        listenerCore.attach(nioChannel: serverChannel)

        return Listener<Message>(port: port, core: listenerCore)
    }
}

#endif
