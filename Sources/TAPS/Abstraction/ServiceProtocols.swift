// ServiceProtocols.swift
// RFC-compliant service protocols

import AsyncAlgorithms
import ServiceLifecycle

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
