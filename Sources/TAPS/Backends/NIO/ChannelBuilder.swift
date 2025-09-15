//
//  ChannelBuilder.swift
//  TAPS
//
//  Created by Joannis Orlandos on 09/09/2025.
//

internal import NIO

@resultBuilder internal struct ProtocolStackBuilder<InboundOut, OutboundOut> {
  // MARK: First block handlers
  @_disfavoredOverload
  internal static func buildPartialBlock<PartialInboundOut, PartialOutboundIn>(
    first subprotocol: ConnectionSubprotocol<
      InboundOut, PartialInboundOut, PartialOutboundIn, OutboundOut
    >
  ) -> ProtocolStack<InboundOut, PartialInboundOut, PartialOutboundIn, OutboundOut> {
    ProtocolStack.unverified(subprotocol.handlers)
  }

  internal static func buildPartialBlock<Handler: ChannelDuplexHandler>(
    first handler: Handler
  ) -> ProtocolStack<InboundOut, Handler.InboundOut, Handler.OutboundIn, OutboundOut> {
    ProtocolStack.unverified {
      [handler]
    }
  }

  @_disfavoredOverload
  internal static func buildPartialBlock<Handler: ChannelInboundHandler>(
    first handler: Handler
  ) -> ProtocolStack<InboundOut, Handler.InboundOut, OutboundOut, OutboundOut>
  where InboundOut == Handler.InboundIn {
    ProtocolStack.unverified {
      [handler]
    }
  }

  @_disfavoredOverload
  internal static func buildPartialBlock<Handler: ChannelOutboundHandler>(
    first handler: Handler
  ) -> ProtocolStack<InboundOut, InboundOut, Handler.OutboundIn, OutboundOut>
  where OutboundOut == Handler.OutboundOut {
    ProtocolStack.unverified {
      [handler]
    }
  }

  // MARK: Accumulated Handlers (non-optional)
  internal static func buildPartialBlock<
    Handler: ChannelDuplexHandler
  >(
    accumulated base: ProtocolStack<
      InboundOut, Handler.InboundIn, Handler.OutboundOut, OutboundOut
    >,
    next handler: Handler
  ) -> ProtocolStack<InboundOut, Handler.InboundOut, Handler.OutboundIn, OutboundOut> {
    ProtocolStack.unverified {
      base.handlers() + [handler]
    }
  }

  internal static func buildPartialBlock<
    PreviousInboundOut,
    PreviousOutboundIn,
    NewInboundOut,
    NewOutboundIn
  >(
    accumulated base: ProtocolStack<
      InboundOut, PreviousInboundOut, PreviousOutboundIn, OutboundOut
    >,
    next stack: ProtocolStack<PreviousInboundOut, NewInboundOut, NewOutboundIn, PreviousOutboundIn>
  ) -> ProtocolStack<InboundOut, NewInboundOut, NewOutboundIn, OutboundOut> {
    ProtocolStack.unverified {
      base.handlers() + stack.handlers()
    }
  }

  internal static func buildPartialBlock<
    PreviousInboundOut,
    PreviousOutboundIn,
    NewInboundOut,
    NewOutboundIn
  >(
    accumulated base: ProtocolStack<
      InboundOut, PreviousInboundOut, PreviousOutboundIn, OutboundOut
    >,
    next stack: ConnectionSubprotocol<
      PreviousInboundOut, NewInboundOut, NewOutboundIn, PreviousOutboundIn
    >
  ) -> ProtocolStack<InboundOut, NewInboundOut, NewOutboundIn, OutboundOut> {
    ProtocolStack.unverified {
      base.handlers() + stack.handlers()
    }
  }

  @_disfavoredOverload
  internal static func buildPartialBlock<
    PartialIn, PartialOut,
    Handler: ChannelInboundHandler
  >(
    accumulated base: ProtocolStack<InboundOut, PartialIn, PartialOut, OutboundOut>,
    next handler: Handler
  ) -> ProtocolStack<InboundOut, Handler.InboundOut, PartialOut, OutboundOut>
  where PartialIn == Handler.InboundIn {
    ProtocolStack.unverified {
      base.handlers() + [handler]
    }
  }

  @_disfavoredOverload
  internal static func buildPartialBlock<
    PartialIn, PartialOut,
    Handler: ChannelOutboundHandler
  >(
    accumulated base: ProtocolStack<InboundOut, PartialIn, PartialOut, OutboundOut>,
    next handler: Handler
  ) -> ProtocolStack<InboundOut, PartialIn, Handler.OutboundIn, OutboundOut>
  where PartialOut == Handler.OutboundOut {
    ProtocolStack.unverified {
      base.handlers() + [handler]
    }
  }

  @_disfavoredOverload
  internal static func buildPartialBlock<PartialOut, Decoder: ByteToMessageDecoder>(
    accumulated base: ProtocolStack<InboundOut, ByteBuffer, PartialOut, OutboundOut>,
    next decoder: Decoder
  ) -> ProtocolStack<InboundOut, Decoder.InboundOut, PartialOut, OutboundOut> {
    ProtocolStack.unverified {
      base.handlers() + [ByteToMessageHandler(decoder)]
    }
  }

  @_disfavoredOverload
  internal static func buildPartialBlock<PartialIn, Encoder: MessageToByteEncoder>(
    accumulated base: ProtocolStack<InboundOut, PartialIn, ByteBuffer, OutboundOut>,
    next encoder: Encoder
  ) -> ProtocolStack<InboundOut, PartialIn, Encoder.OutboundIn, OutboundOut> {
    ProtocolStack.unverified {
      base.handlers() + [MessageToByteHandler(encoder)]
    }
  }

  // MARK: Optional handlers
  internal static func buildOptional<T>(_ component: T?) -> T? {
    component
  }

  internal static func buildPartialBlock<
    Handler: ChannelDuplexHandler
  >(
    accumulated base: ProtocolStack<
      InboundOut, Handler.InboundIn, Handler.OutboundOut, OutboundOut
    >,
    next handler: Handler?
  ) -> ProtocolStack<InboundOut, Handler.InboundOut, Handler.OutboundIn, OutboundOut>
  where Handler.InboundIn == Handler.InboundOut, Handler.OutboundIn == Handler.OutboundOut {
    ProtocolStack.unverified {
      if let handler {
        return base.handlers() + [handler]
      } else {
        return base.handlers()
      }
    }
  }

  @_disfavoredOverload
  internal static func buildPartialBlock<
    PartialOutboundIn,
    Handler: ChannelInboundHandler
  >(
    accumulated base: ProtocolStack<
      InboundOut, PartialOutboundIn, Handler.OutboundOut, OutboundOut
    >,
    next handler: Handler?
  ) -> ProtocolStack<InboundOut, Handler.InboundOut, PartialOutboundIn, OutboundOut>
  where Handler.InboundIn == Handler.InboundOut {
    ProtocolStack.unverified {
      if let handler {
        return base.handlers() + [handler]
      } else {
        return base.handlers()
      }
    }
  }

  @_disfavoredOverload
  internal static func buildPartialBlock<
    PartialInboundOut,
    Handler: ChannelOutboundHandler
  >(
    accumulated base: ProtocolStack<
      InboundOut, PartialInboundOut, Handler.OutboundOut, OutboundOut
    >,
    next handler: Handler?
  ) -> ProtocolStack<InboundOut, PartialInboundOut, Handler.OutboundIn, OutboundOut>
  where Handler.OutboundIn == Handler.OutboundOut {
    ProtocolStack.unverified {
      if let handler {
        return base.handlers() + [handler]
      } else {
        return base.handlers()
      }
    }
  }

  internal static func buildPartialBlock<
    PartialInboundOut,
    PartialOutboundIn
  >(
    accumulated base: ProtocolStack<InboundOut, PartialInboundOut, PartialOutboundIn, OutboundOut>,
    next stack: ProtocolStack<InboundOut, PartialInboundOut, PartialOutboundIn, OutboundOut>?
  ) -> ProtocolStack<InboundOut, PartialInboundOut, PartialOutboundIn, OutboundOut> {
    ProtocolStack.unverified {
      if let stack {
        return base.handlers() + stack.handlers()
      } else {
        return base.handlers()
      }
    }
  }

  // MARK: Final result

  internal static func buildFinalResult<Input, Output>(
    _ component: ProtocolStack<InboundOut, Output, Input, OutboundOut>
  ) -> ProtocolStack<InboundOut, Output, Input, OutboundOut> {
    component
  }
}

internal final class IODataOutboundEncoder: ChannelOutboundHandler {
  internal typealias OutboundIn = ByteBuffer
  internal typealias OutboundOut = IOData

  internal init() {}

  internal func write(
    context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?
  ) {
    context.write(data, promise: nil)
  }
}

internal final class IODataOutboundDecoder: ChannelOutboundHandler {
  internal typealias OutboundIn = IOData
  internal typealias OutboundOut = ByteBuffer

  internal init() {}

  internal func write(
    context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?
  ) {
    context.write(data, promise: nil)
  }
}

internal final class IODataInboundEncoder: ChannelInboundHandler {
  internal typealias InboundIn = ByteBuffer
  internal typealias InboundOut = IOData

  internal init() {}

  internal func channelRead(context: ChannelHandlerContext, data: NIOAny) {
    context.fireChannelRead(data)
  }
}

internal final class IODataInboundDecoder: ChannelInboundHandler {
  internal typealias InboundIn = IOData
  internal typealias InboundOut = ByteBuffer

  internal init() {}

  internal func channelRead(context: ChannelHandlerContext, data: NIOAny) {
    context.fireChannelRead(data)
  }
}

internal final class IODataDuplexHandler: ChannelDuplexHandler {
  internal typealias InboundIn = IOData
  internal typealias InboundOut = ByteBuffer
  internal typealias OutboundIn = ByteBuffer
  internal typealias OutboundOut = IOData

  internal init() {}

  internal func channelRead(context: ChannelHandlerContext, data: NIOAny) {
    context.fireChannelRead(data)
  }

  internal func write(
    context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?
  ) {
    context.write(data, promise: nil)
  }
}

internal final class NetworkBytesDuplexHandler: ChannelDuplexHandler {
  internal typealias InboundIn = _NetworkBytes
  internal typealias InboundOut = NetworkInputBytes
  internal typealias OutboundIn = NetworkOutputBytes
  internal typealias OutboundOut = _NetworkBytes

  internal init() {}

  internal func channelRead(context: ChannelHandlerContext, data: NIOAny) {
    let buffer = unwrapInboundIn(data)
    let networkBytes = NetworkInputBytes(buffer: buffer)
    context.fireChannelRead(wrapInboundOut(networkBytes))
  }

  internal func write(
    context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?
  ) {
    let networkBytes = unwrapOutboundIn(data)
    context.write(wrapOutboundOut(networkBytes.buffer), promise: nil)
  }
}
