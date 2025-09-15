import Observation

public struct MetricsBluetoothService<Entity: Observable & Sendable>: BluetoothServiceProtocol {
    public let id: BluetoothUUID
    internal let entity: Entity
    internal let _writeCharacteristics: @Sendable (inout BluetoothCharacteristicsWriter) async throws -> Void
    
    public struct Observation<Value: Sendable>: @unchecked Sendable {
        internal let keyPath: KeyPath<Entity, Value>
        internal let characteristic: BluetoothCharacteristic<Value>
        
        public init(
            keyPath: KeyPath<Entity, Value>,
            characteristic: BluetoothCharacteristic<Value>
        ) {
            self.keyPath = keyPath
            self.characteristic = characteristic
        }
    }
    
    public init<each Value: Sendable>(
        serviceId: BluetoothUUID,
        observing entity: Entity,
        observations: repeat Observation<each Value>
    ) {
        @Sendable func _writeCharacteristics(
            into writer: inout BluetoothCharacteristicsWriter
        ) async throws {
            @Sendable func observe<ObservedValue: Sendable>(
                observation: Observation<ObservedValue>,
                handle: @Sendable (ObservedValue) async throws -> Void
            ) async throws {
                let values = Observations.untilFinished {
                    .next(entity[keyPath: observation.keyPath])
                }
                
                for await value in values {
                    try Task.checkCancellation()
                    // Re-apply observation
                    try await handle(value)
                }
            }
            
            for observation in repeat each observations {
                let initialValue = entity[keyPath: observation.keyPath]
                
                try await writer.add(
                    characteristic: observation.characteristic,
                    initialValue: initialValue,
                    withValues: { handle in
                        try await observe(
                            observation: observation,
                            handle: handle
                        )
                    },
                    permissions: .read,
                    properties: .read
                )
            }
        }
        
        self.id = serviceId
        self.entity = entity
        self._writeCharacteristics = _writeCharacteristics
    }
    
    public func writeCharacteristics(into writer: inout BluetoothCharacteristicsWriter) async throws {
        try await _writeCharacteristics(&writer)
    }
}
