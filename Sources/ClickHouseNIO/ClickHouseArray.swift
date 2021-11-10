//
//  ClickHouseArray.swift
//  ClickHouseNIO
//
//  Created by Patrick Zippenfenig on 2019-12-01.
//

import Foundation
import NIO

public struct ClickHouseColumn {
    public let name: String
    public let values: ClickHouseDataTypeArray
    
    public var count: Int { return values.count }
    
    public init(_ name: String, _ values: ClickHouseDataTypeArray) {
        self.name = name
        self.values = values
    }
    
    public func merge(with: [ClickHouseColumn]) throws -> ClickHouseColumn {
        return ClickHouseColumn(name, try values.merge(with: with.map({$0.values})) )
    }
}

public protocol ClickHouseDataTypeArray {
    var count: Int { get }
    
    func merge(with: [ClickHouseDataTypeArray]) throws -> ClickHouseDataTypeArray
    
    func writeTo(buffer: inout ByteBuffer, type: ClickHouseTypeName, name: String)
    
    static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: Int?) -> ClickHouseDataTypeArray?
}


extension Array: ClickHouseDataTypeArray where Element: ClickHouseDataType {
    public func merge(with: [ClickHouseDataTypeArray]) throws -> ClickHouseDataTypeArray {
        let sameType = try with.map { anyArray -> [Element] in
            guard let column = anyArray as? [Element] else {
                throw ClickHouseError.invalidDataType
            }
            return column
        }
        return ([self] + sameType).flatMap({$0})
    }
    
    public func writeTo(buffer: inout ByteBuffer, type: ClickHouseTypeName, name: String) {
        assert(type.string == Element.getClickHouseTypeName(fixedLength: type.fixedLength).string)
        
        buffer.writeClickHouseString(name)
        buffer.writeClickHouseString(type.string)
        Element.writeTo(buffer: &buffer, array: self, fixedLength: type.fixedLength)
    }
    
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: Int?) -> ClickHouseDataTypeArray? {
        return Element.readFrom(buffer: &buffer, numRows: numRows, fixedLength: fixedLength)
    }
}



public indirect enum ClickHouseTypeName {
    case float
    case float64
    case int8
    case int16
    case int32
    case int64
    case uint8
    case uint16
    case uint32
    case uint64
    case uuid
    case fixedString(Int)
    case string
    case nullable(ClickHouseTypeName)
    
    public init?(_ type: String) {
        if type.starts(with: "Nullable(") {
            let subTypeName = String(type.dropFirst("Nullable(".count).dropLast())
            guard let subType = ClickHouseTypeName(subTypeName) else {
                return nil
            }
            self = .nullable(subType)
        }
        else if type.starts(with: "FixedString(") {
            guard let len = Int(type.dropFirst("FixedString(".count).dropLast()) else {
                return nil
            }
            self = .fixedString(len)
        }
        else {
            switch type {
            case "Float32":
                self = .float
            case "Float64":
                self = .float64
            case "Int8":
                self = .int8
            case "Int16":
                self = .int16
            case "Int32":
                self = .int32
            case "Int64":
                self = .int64
            case "UInt8":
                self = .uint8
            case "UInt16":
                self = .uint16
            case "UInt32":
                self = .uint32
            case "UInt64":
                self = .uint64
            case "UUID":
                self = .uuid
            case "String":
                self = .string
            default:
                return nil
            }
        }
    }
    public var fixedLength: Int? {
        switch self {
        case .fixedString(let len):
            return len
        default:
            return nil
        }
    }
    
    public var string: String {
        switch self {
        case .float:
            return "Float32"
        case .uint8:
            return "UInt8"
        case .int32:
            return "Int32"
        case .int64:
            return "Int64"
        case .uint64:
            return "UInt64"
        case .fixedString(let len):
            return "FixedString(\(len))"
        case .string:
            return "String"
        case .uuid:
            return "UUID"
        case .float64:
            return "Float64"
        case .int8:
            return "Int8"
        case .int16:
            return "Int16"
        case .uint16:
            return "UInt16"
        case .uint32:
            return "UInt32"
        case .nullable(let subtype):
            return "Nullable(\(subtype.string))"
        }
    }
    
    public var primitiveType: ClickHouseDataTypeArray.Type {
        switch self {
        case .float:
            return [Float].self
        case .float64:
            return [Double].self
        case .int8:
            return [Int8].self
        case .int16:
            return [Int16].self
        case .int32:
            return [Int32].self
        case .int64:
            return [Int64].self
        case .uint8:
            return [UInt8].self
        case .uint16:
            return [UInt16].self
        case .uint32:
            return [UInt32].self
        case .uint64:
            return [UInt64].self
        case .uuid:
            return [UUID].self
        case .fixedString(_):
            return [String].self
        case .string:
            return [String].self
        case .nullable(let type):
            switch type {
            case .float:
                return [Float?].self
            case .float64:
                return [Double?].self
            case .int8:
                return [Int8?].self
            case .int16:
                return [Int16?].self
            case .int32:
                return [Int32?].self
            case .int64:
                return [Int64?].self
            case .uint8:
                return [UInt8?].self
            case .uint16:
                return [UInt16?].self
            case .uint32:
                return [UInt32?].self
            case .uint64:
                return [UInt64?].self
            case .uuid:
                return [UUID?].self
            case .fixedString(_):
                return [String?].self
            case .string:
                return [String?].self
            case .nullable(_):
                fatalError("Nullable cannot be nested")
            }
        }
        
    }
}

public protocol ClickHouseDataType {
    /// Used by ORM to determinte the type of a given array
    static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName
    
    /// Write an array of this type to a bytebuffer
    static func writeTo(buffer: inout ByteBuffer, array: [Self], fixedLength: Int?)
    
    /// Return nil for more data. Moved buffer forward if sucessfull
    static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: Int?) -> [Self]?
    
    /// Default value. Used for nullable values
    static var clickhouseDefault: Self { get }
}

extension String: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: Int?) -> [String]? {
        if let fixedLength = fixedLength {
            var strings = [String]()
            strings.reserveCapacity(numRows)
            for _ in 0..<numRows {
                guard let str = buffer.readString(length: fixedLength) else {
                    return nil
                }
                if let pos = str.firstIndex(of: "\0") {
                    // String contains zero byte. Truncate it
                    strings.append(String(str[..<pos]))
                } else {
                    strings.append(str)
                }
            }
            return strings
        }
        
        return buffer.readClickHouseStrings(numRows: numRows)
    }
    
    public func clickhouseReadValue(buffer: inout ByteBuffer, fixedLength: Int?) -> String? {
        return buffer.readClickHouseString()
    }
    
    public static var clickhouseDefault: String {
        return ""
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [String], fixedLength: Int?) {
        if let length = fixedLength {
            buffer.writeClickHouseFixedStrings(array, length: length)
        } else {
            buffer.writeClickHouseStrings(array)
        }
    }
    
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        if let len = fixedLength {
            return .fixedString(len)
        }
        return .string
    }
}

extension Int8: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: Int?) -> [Int8]? {
        return buffer.readIntegerArray(numRows: numRows)
    }
    
    public static var clickhouseDefault: Int8 {
        return 0
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [Int8], fixedLength: Int?) {
        buffer.writeIntegerArray(array)
    }
    
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .int8
    }
}

extension Int16: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: Int?) -> [Int16]? {
        return buffer.readIntegerArray(numRows: numRows)
    }
    
    public static var clickhouseDefault: Int16 {
        return 0
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [Int16], fixedLength: Int?) {
        buffer.writeIntegerArray(array)
    }
    
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .int16
    }
}

extension Int32: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: Int?) -> [Int32]? {
        return buffer.readIntegerArray(numRows: numRows)
    }
    
    public static var clickhouseDefault: Int32 {
        return 0
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [Int32], fixedLength: Int?) {
        buffer.writeIntegerArray(array)
    }
    
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .int32
    }
}

extension Int64: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: Int?) -> [Int64]? {
        return buffer.readIntegerArray(numRows: numRows)
    }
    
    public static var clickhouseDefault: Int64 {
        return 0
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [Int64], fixedLength: Int?) {
        buffer.writeIntegerArray(array)
    }
    
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .int64
    }
}

extension UInt8: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: Int?) -> [UInt8]? {
        return buffer.readIntegerArray(numRows: numRows)
    }
    
    public static var clickhouseDefault: UInt8 {
        return 0
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [UInt8], fixedLength: Int?) {
        buffer.writeIntegerArray(array)
    }
    
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .uint8
    }
}

extension UInt16: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: Int?) -> [UInt16]? {
        return buffer.readIntegerArray(numRows: numRows)
    }
    
    public static var clickhouseDefault: UInt16 {
        return 0
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [UInt16], fixedLength: Int?) {
        buffer.writeIntegerArray(array)
    }
    
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .uint16
    }
}

extension UInt32: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: Int?) -> [UInt32]? {
        return buffer.readIntegerArray(numRows: numRows)
    }
    
    public static var clickhouseDefault: UInt32 {
        return 0
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [UInt32], fixedLength: Int?) {
        buffer.writeIntegerArray(array)
    }
    
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .uint32
    }
}

extension UInt64: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: Int?) -> [UInt64]? {
        return buffer.readIntegerArray(numRows: numRows)
    }
    
    public static var clickhouseDefault: UInt64 {
        return 0
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [UInt64], fixedLength: Int?) {
        buffer.writeIntegerArray(array)
    }
    
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .uint64
    }
}

extension Float: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: Int?) -> [Float]? {
        return buffer.readUnsafeGenericArray(numRows: numRows)
    }
    
    public static var clickhouseDefault: Float {
        return 0
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [Float], fixedLength: Int?) {
        let _ = array.withUnsafeBytes {
            buffer.writeBytes($0)
        }
    }
    
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .float
    }
}

extension Double: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: Int?) -> [Double]? {
        return buffer.readUnsafeGenericArray(numRows: numRows)
    }
    
    public static var clickhouseDefault: Double {
        return 0
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [Double], fixedLength: Int?) {
        let _ = array.withUnsafeBytes {
            buffer.writeBytes($0)
        }
    }
    
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .float64
    }
}

extension UUID: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: Int?) -> [UUID]? {
        return buffer.readUuidArray(numRows: numRows, endianness: .little)
    }
    
    public static var clickhouseDefault: UUID {
        return .init(uuidString: "00000000-0000-0000-0000-000000000000")!
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [UUID], fixedLength: Int?) {
        buffer.writeUuidArray(array, endianness: .little)
    }
    
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .uuid
    }
}

extension Optional: ClickHouseDataType where Wrapped: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: Int?) -> [Optional<Wrapped>]? {
        var bufferCopy = buffer
        
        guard bufferCopy.readableBytes >= (1) * numRows else {
            return nil
        }
        var isnull = [Bool]()
        isnull.reserveCapacity(numRows)
        for _ in 0..<numRows {
            guard let set: UInt8 = bufferCopy.readInteger(endianness: .little) else {
                return nil // need more data
            }
            isnull.append(set == 1)
        }
        
        guard let data = Wrapped.readFrom(buffer: &bufferCopy, numRows: numRows, fixedLength: fixedLength) else {
            return nil
        }
        let mapped = data.enumerated().map({
            isnull[$0.offset] ? nil : $0.element
        })
        buffer = bufferCopy
        return mapped
    }
    
    public static var clickhouseDefault: Optional<Wrapped> {
        return nil
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [Optional<Wrapped>], fixedLength: Int?) {
        buffer.reserveCapacity(array.count * (1) + buffer.writableBytes)
        // Frist write one array with 0/1 for nullable, then data
        for element in array {
            if element == nil {
                buffer.writeInteger(UInt8(1), endianness: .little)
            } else {
                buffer.writeInteger(UInt8(0), endianness: .little)
            }
        }
        let mapped: [Wrapped] = array.map {
            guard let value = $0 else {
                return Wrapped.clickhouseDefault
            }
            return value
        }
        Wrapped.writeTo(buffer: &buffer, array: mapped, fixedLength: fixedLength)
    }
    
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .nullable(Wrapped.getClickHouseTypeName(fixedLength: fixedLength))
    }
}
