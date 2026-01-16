// ServiceProtocols.swift
// RFC-compliant service protocols

import AsyncAlgorithms
public import ServiceLifecycle

/// Base protocol for client services
public protocol ClientServiceProtocol: Sendable {
  associatedtype Parameters: Sendable
  associatedtype Client: ServiceLifecycle.Service

  /// Create connection with given parameters
  func withConnection<T: Sendable>(
    parameters: Parameters,
    context: TAPSContext,
    perform: @escaping @Sendable (Client) async throws -> T
  ) async throws -> T
}

/// Parameters with default values
public protocol ParametersWithDefault: Sendable {
  static var defaultParameters: Self { get }
}

/// Base protocol for server services
public protocol ServerServiceProtocol: Sendable {
  associatedtype Parameters: Sendable
  associatedtype Server: DuplexServerProtocol

  /// Create server with given parameters
  func withServer<T: Sendable>(
    parameters: Parameters,
    context: TAPSContext,
    perform: @escaping @Sendable (Server) async throws -> T
  ) async throws -> T
}

/// Base protocol for datagram (UDP) services
public protocol DatagramServiceProtocol: Sendable {
  associatedtype Parameters: Sendable
  associatedtype Socket: DatagramSocketProtocol

  /// Create socket with given parameters
  func withSocket<T: Sendable>(
    parameters: Parameters,
    context: TAPSContext,
    perform: @escaping @Sendable (Socket) async throws -> T
  ) async throws -> T
}
