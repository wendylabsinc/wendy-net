// Connection.swift
// Actor-based connection protocols

import AsyncAlgorithms
import ServiceLifecycle

#if canImport(NIOCore)
  internal import NIOCore
#endif

/// Base protocol for all connection types
public protocol DuplexClientProtocol<InboundMessage, OutboundMessage>: ServiceLifecycle.Service,
  Sendable
{
  associatedtype InboundMessage: Sendable
  associatedtype OutboundMessage: Sendable
  associatedtype ConnectionError: Swift.Error
}

/// Protocol for server connections that accept clients
public protocol DuplexServerProtocol<Client>: Sendable {
  associatedtype Client: DuplexClientProtocol
  associatedtype ConnectionError: Error

  func withEachClient(
    _ acceptClient: @Sendable @escaping (Client) async throws(CancellationError) -> Void
  ) async throws(ConnectionError)
}

public struct ConnectionSubprotocol<
  InboundIn: Sendable,
  InboundOut: Sendable,
  OutboundIn: Sendable,
  OutboundOut: Sendable
>: @unchecked Sendable {
  #if canImport(NIOCore)
    var handlers: () -> [any ChannelHandler]

    internal init() {
      self.handlers = {
        []
      }
    }

    internal init(
      _ inboundIn: InboundIn.Type = InboundIn.self,
      _ inboundOut: OutboundOut.Type = OutboundOut.self,
      @ProtocolStackBuilder<InboundIn, OutboundOut> validated build:
        @escaping @Sendable () -> ProtocolStack<InboundIn, InboundOut, OutboundIn, OutboundOut>
    ) {
      self.handlers = {
        build().handlers()
      }
    }
  #endif
}

public struct ProtocolStack<
  InboundIn: Sendable,
  InboundOut: Sendable,
  OutboundIn: Sendable,
  OutboundOut: Sendable
>: @unchecked Sendable {
  #if canImport(NIOCore)
    var handlers: () -> [any ChannelHandler]
  #endif

  internal init() {
    self.handlers = { [] }
  }

  internal init(
    @ProtocolStackBuilder<InboundIn, OutboundOut> validated build:
      @escaping @Sendable () -> ProtocolStack<InboundIn, InboundOut, OutboundIn, OutboundOut>
  ) {
    self.handlers = {
      build().handlers()
    }
  }
}

extension ProtocolStack {
  internal static func unverified(
    _ handlers: @escaping () -> [any ChannelHandler]
  ) -> ProtocolStack {
    var subprotocol = ProtocolStack()
    subprotocol.handlers = handlers
    return subprotocol
  }
}
