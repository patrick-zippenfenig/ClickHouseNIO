//
//  Exception.swift
//  ClickHouseNIO
//
//  Created by Patrick Zippenfenig on 2019-11-26.
//

import Foundation
import NIO

public struct ExceptionMessage: Error, Equatable {
    public let code: UInt32
    public let name: String
    public let displayText: String
    public let stackTrace: String
    // using array to nest structs
    public let nested: [Self]

    init?(from buffer: inout ByteBuffer) {
        guard let code: UInt32 = buffer.readInteger(),
            let name = buffer.readClickHouseString(),
            let displayText = buffer.readClickHouseString(),
            let stackTrace = buffer.readClickHouseString(),
            let hasNested: Int8 = buffer.readInteger() else {
            return nil
        }
        if hasNested == 1 {
            guard let nested = Self(from: &buffer) else {
                return nil
            }
            self.nested = [nested]
        } else {
            self.nested = []
        }
        self.code = code
        self.name = name
        self.displayText = displayText
        self.stackTrace = stackTrace
    }
}
