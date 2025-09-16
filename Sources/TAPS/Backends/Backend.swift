#if canImport(NIOCore)
  internal import NIOCore

  typealias _NetworkBytes = ByteBuffer
  typealias _NetworkInputBytes = ByteBuffer
  typealias _NetworkOutputBytes = IOData
#elseif canImport(Network)
  import Foundation

  typealias _NetworkBytes = Data
  typealias _NetworkInputBytes = Data
  typealias _NetworkOutputBytes = Data
#endif

public enum Endianness: Sendable {
  case little, big

  #if canImport(NIOCore)
    var nio: NIOCore.Endianness {
      switch self {
      case .little: .little
      case .big: .big
      }
    }
  #endif
}

public struct NetworkInputBytes: Sendable {
  public enum Error: Swift.Error {
    case notEnoughData
    case outOfBounds
    case unknown
  }

  internal var buffer: _NetworkBytes

  public var isEmpty: Bool {
    buffer.readableBytes == 0
  }

  internal mutating func append(contentsOf input: NetworkInputBytes) {
    buffer.writeImmutableBuffer(input.buffer)
  }

  internal mutating func discardReadBytes() {
    buffer.discardReadBytes()
  }
}

extension String {
  public init(bytes: NetworkInputBytes) {
    self = bytes.buffer.withUnsafeReadableBytes { pointer in
      String(
        decoding: pointer,
        as: Unicode.UTF8.self
      )
    }
  }
}

public struct NetworkOutputBytes: Sendable {
  public enum Error: Swift.Error {
    case lengthPrefixSpaceExceeded
  }

  internal var buffer: _NetworkBytes

  internal init(buffer: _NetworkBytes) {
    self.buffer = buffer
  }

  public init(string: String) {
    self.buffer = _NetworkBytes(string: string)
  }

  public mutating func writeLengthPrefixed<F: FixedWidthInteger>(
    endianness: Endianness,
    as type: F.Type = F.self,
    write: (inout NetworkOutputBytes) throws(Error) -> Void
  ) throws(NetworkOutputBytes.Error) {
    do {
      try buffer.writeLengthPrefixed(
        endianness: endianness.nio,
        as: F.self
      ) { buffer in
        let oldSize = buffer.writerIndex
        var output = NetworkOutputBytes(buffer: buffer)
        try write(&output)
        buffer = output.buffer
        let newSize = buffer.writerIndex
        return newSize - oldSize
      }
    } catch let error as Error {
      throw error
    } catch {
      throw Error.lengthPrefixSpaceExceeded
    }
  }

  public mutating func writeInteger<F: FixedWidthInteger>(
    _ integer: F,
    endianness: Endianness,
    as type: F.Type = F.self
  ) {
    buffer.writeInteger(integer)
  }
}

public protocol NetworkSerializable: Sendable {
  func serialize(into output: inout NetworkOutputBytes) throws(NetworkOutputBytes.Error)
}

public protocol NetworkDeserializable: Sendable {
  static func deserialize(from input: inout NetworkInputBytes) throws(NetworkInputBytes.Error)
    -> Self
}

extension NetworkInputBytes: NetworkSerializable, NetworkDeserializable {
  public func serialize(
    into output: inout NetworkOutputBytes
  ) {
    output.buffer.writeImmutableBuffer(self.buffer)
  }

  public static func deserialize(from input: inout NetworkInputBytes) -> NetworkInputBytes {
    NetworkInputBytes(buffer: input.buffer)
  }
}
