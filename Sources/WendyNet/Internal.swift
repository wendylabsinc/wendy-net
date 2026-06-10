// Non-public plumbing shared by both backends. API.swift carries the public
// surface; everything internal-only lives here.

let wendyNetMaximumMessageLength = 1024

// MARK: - Internal step results

enum InboundStep<Message: Sendable>: Sendable {
    case message(Message)
    case end
    case failure(WendyNetError)
}

enum OutboundStep: Sendable {
    case accepted
    case failure(WendyNetError)
}

enum AcceptedStep<Message: Sendable>: Sendable {
    case channel(Channel<Message>)
    case end
    case failure(WendyNetError)
}

// MARK: - Idle timer

/// Backend-agnostic idle-timeout loop for a UDP association, run as a child of
/// the channel's `executeThenClose` scope so it is cancelled when the channel
/// is done. `remaining` reports the time left until the deadline (`timeout`
/// since last activity), `nil` once the association has ended, or `<= .zero`
/// once it has expired; `sleep` waits for a `Duration`, returning `false` if
/// cancelled; `evict` reclaims the idle association. Re-reading `remaining`
/// after each sleep reschedules precisely against the latest activity.
func runIdleTimer(
    remaining: @Sendable () -> Duration?,
    sleep: @Sendable (Duration) async -> Bool,
    evict: @Sendable () async -> Void
) async {
    while true {
        guard let left = remaining() else { return }
        if left <= .zero {
            await evict()
            return
        }
        if await sleep(left) == false { return }  // cancelled
    }
}

// MARK: - Pipeline closures

/// Closure bundle produced by a pipeline factory.
struct PipelineClosures<Message> {
    let decode: (ByteBuffer, (Message) -> Void, (WendyNetError) -> Void) -> Void
    let encode: (Message) -> ByteBuffer
    let started: (ConnectionContext) -> Void
}

/// Closure bundle for a framer (ByteBuffer -> ByteBuffer, stream transports only).
struct FramerClosures {
    let decode: (ByteBuffer, (ByteBuffer) -> Void, (WendyNetError) -> Void) -> Void
    let encode: (ByteBuffer) -> ByteBuffer
    let started: (ConnectionContext) -> Void

    /// Wrap a pipeline's closures so the framer runs first on inbound
    /// and last on outbound.
    func composing<Message>(_ inner: PipelineClosures<Message>) -> PipelineClosures<Message> {
        let framerDecode = self.decode
        let framerEncode = self.encode
        let framerStarted = self.started
        return PipelineClosures<Message>(
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

// MARK: - Bootstrap configuration

/// Shared configuration backing both `ClientBootstrap` and `ServerBootstrap`.
struct BootstrapConfig<Message: Sendable>: Sendable {
    var security: SecurityMode = .insecure
    var reliability: Reliability = .reliable
    let pipelineFactory: @Sendable () -> PipelineClosures<Message>
    var framerFactory: (@Sendable () -> FramerClosures)? = nil

    /// The identity pipeline: raw bytes through, no decode/encode transform.
    static func passthrough() -> BootstrapConfig<ByteBuffer> {
        BootstrapConfig<ByteBuffer>(
            pipelineFactory: {
                PipelineClosures(
                    decode: { buf, emit, _ in emit(buf) },
                    encode: { $0 },
                    started: { _ in }
                )
            }
        )
    }

    /// Re-key this config to a new message type, swapping in a fresh pipeline
    /// factory while preserving every other field (so `.pipeline()` can't drop
    /// a previously configured option).
    func reframed<New: Sendable>(
        pipelineFactory: @escaping @Sendable () -> PipelineClosures<New>
    ) -> BootstrapConfig<New> {
        BootstrapConfig<New>(
            security: security,
            reliability: reliability,
            pipelineFactory: pipelineFactory,
            framerFactory: framerFactory
        )
    }
}

/// Wrap a `PipelineStage` factory into framer closures.
func makeFramerClosures<F: PipelineStage & SendableMetatype>(
    _ factory: @escaping @Sendable () -> F
) -> @Sendable () -> FramerClosures where F.Input == ByteBuffer, F.Output == ByteBuffer {
    {
        let framer = factory()
        return FramerClosures(
            decode: { buf, emit, fail in framer.decode(buf, emit, fail) },
            encode: { buf in framer.encode(buf) },
            started: { context in framer.started(context: context) }
        )
    }
}

/// Wrap a `PipelineStage` factory into pipeline closures.
func makePipelineClosures<P: PipelineStage & SendableMetatype>(
    _ build: @escaping @Sendable () -> P
) -> @Sendable () -> PipelineClosures<P.Output> where P.Input == ByteBuffer, P.Output: Sendable {
    {
        let pipeline = build()
        return PipelineClosures(
            decode: { buf, emit, fail in pipeline.decode(buf, emit, fail) },
            encode: { msg in pipeline.encode(msg) },
            started: { context in pipeline.started(context: context) }
        )
    }
}
