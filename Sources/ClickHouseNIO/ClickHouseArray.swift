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
}

public protocol ClickHouseDataType {
    /// Used by ORM to determinte the type of a given array
    static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName
    
    static func writeTo(buffer: inout ByteBuffer, array: [Self], fixedLength: Int?)
}
extension String: ClickHouseDataType {
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
    public static func writeTo(buffer: inout ByteBuffer, array: [Int8], fixedLength: Int?) {
        buffer.writeIntegerArray(array)
    }
    
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .int8
    }
}
extension Int16: ClickHouseDataType {
    public static func writeTo(buffer: inout ByteBuffer, array: [Int16], fixedLength: Int?) {
        buffer.writeIntegerArray(array)
    }
    
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .int16
    }
}
extension Int32: ClickHouseDataType {
    public static func writeTo(buffer: inout ByteBuffer, array: [Int32], fixedLength: Int?) {
        buffer.writeIntegerArray(array)
    }
    
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .int32
    }
}
extension Int64: ClickHouseDataType {
    public static func writeTo(buffer: inout ByteBuffer, array: [Int64], fixedLength: Int?) {
        buffer.writeIntegerArray(array)
    }
    
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .int64
    }
}
extension UInt8: ClickHouseDataType {
    public static func writeTo(buffer: inout ByteBuffer, array: [UInt8], fixedLength: Int?) {
        buffer.writeIntegerArray(array)
    }
    
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .uint8
    }
}
extension UInt16: ClickHouseDataType {
    public static func writeTo(buffer: inout ByteBuffer, array: [UInt16], fixedLength: Int?) {
        buffer.writeIntegerArray(array)
    }
    
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .uint16
    }
}
extension UInt32: ClickHouseDataType {
    public static func writeTo(buffer: inout ByteBuffer, array: [UInt32], fixedLength: Int?) {
        buffer.writeIntegerArray(array)
    }
    
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .uint32
    }
}
extension UInt64: ClickHouseDataType {
    public static func writeTo(buffer: inout ByteBuffer, array: [UInt64], fixedLength: Int?) {
        buffer.writeIntegerArray(array)
    }
    
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .uint64
    }
}
extension Float: ClickHouseDataType {
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
    public static func writeTo(buffer: inout ByteBuffer, array: [UUID], fixedLength: Int?) {
        buffer.writeUuidArray(array, endianness: .little)
    }
    
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .uuid
    }
}

extension Optional: ClickHouseDataType where Wrapped: FixedWidthInteger & ClickHouseDataType {
    public static func writeTo(buffer: inout ByteBuffer, array: [Optional<Wrapped>], fixedLength: Int?) {
        buffer.writeIntegerArray(array)
    }
    
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .nullable(Wrapped.getClickHouseTypeName(fixedLength: fixedLength))
    }
}
    
extension ByteBuffer {
    mutating func toClickHouseArray(type: ClickHouseTypeName, numRows: Int, name: String) -> ClickHouseColumn? {
        switch type {
        case .string:
            guard let strings = readClickHouseStrings(numRows: numRows) else {
                return nil
            }
            return ClickHouseColumn(name, strings)
        case .int64:
            guard let array: [Int64] = readIntegerArray(numRows: numRows) else {
                return nil
            }
            return ClickHouseColumn(name, array)
        case .int32:
            guard let array: [Int32] = readIntegerArray(numRows: numRows) else {
                return nil
            }
            return ClickHouseColumn(name, array)
        case .uint64:
            guard let array: [UInt64] = readIntegerArray(numRows: numRows) else {
                return nil
            }
            return ClickHouseColumn(name, array)
        case .uint8:
            guard let array: [UInt8] = readIntegerArray(numRows: numRows) else {
                return nil
            }
            return ClickHouseColumn(name, array)
        case .float:
            guard let array: [Float] = readUnsafeGenericArray(numRows: numRows) else {
                return nil
            }
            return ClickHouseColumn(name, array)
        case .uuid:
            guard let array = readUuidArray(numRows: numRows, endianness: .little) else {
                return nil
            }
            return ClickHouseColumn(name, array)
        case .fixedString(let fixedStringLength):
            var strings = [String]()
            strings.reserveCapacity(numRows)
            for _ in 0..<numRows {
                guard let str = readString(length: fixedStringLength) else {
                    return nil
                }
                if let pos = str.firstIndex(of: "\0") {
                    // String contains zero byte. Truncate it
                    strings.append(String(str[..<pos]))
                } else {
                    strings.append(str)
                }
            }
            return ClickHouseColumn(name, strings)
        case .float64:
            guard let array: [Double] = readUnsafeGenericArray(numRows: numRows) else {
                return nil
            }
            return ClickHouseColumn(name, array)
        case .int8:
            guard let array: [Int8] = readIntegerArray(numRows: numRows) else {
                return nil
            }
            return ClickHouseColumn(name, array)
        case .int16:
            guard let array: [Int16] = readIntegerArray(numRows: numRows) else {
                return nil
            }
            return ClickHouseColumn(name, array)
        case .uint16:
            guard let array: [UInt16] = readIntegerArray(numRows: numRows) else {
                return nil
            }
            return ClickHouseColumn(name, array)
        case .uint32:
            guard let array: [UInt32] = readIntegerArray(numRows: numRows) else {
                return nil
            }
            return ClickHouseColumn(name, array)
        case .nullable(let subtype):
            switch subtype {
            case .float:
                fatalError("Not supported")
            case .float64:
                fatalError("Not supported")
            case .int8:
                guard let array: [Int8?] = readOptionalIntegerArray(numRows: numRows) else {
                    return nil
                }
                return ClickHouseColumn(name, array)
            case .int16:
                guard let array: [Int16?] = readOptionalIntegerArray(numRows: numRows) else {
                    return nil
                }
                return ClickHouseColumn(name, array)
            case .int32:
                guard let array: [Int32?] = readOptionalIntegerArray(numRows: numRows) else {
                    return nil
                }
                return ClickHouseColumn(name, array)
            case .int64:
                guard let array: [Int64?] = readOptionalIntegerArray(numRows: numRows) else {
                    return nil
                }
                return ClickHouseColumn(name, array)
            case .uint8:
                guard let array: [UInt8?] = readOptionalIntegerArray(numRows: numRows) else {
                    return nil
                }
                return ClickHouseColumn(name, array)
            case .uint16:
                guard let array: [UInt16?] = readOptionalIntegerArray(numRows: numRows) else {
                    return nil
                }
                return ClickHouseColumn(name, array)
            case .uint32:
                guard let array: [UInt32?] = readOptionalIntegerArray(numRows: numRows) else {
                    return nil
                }
                return ClickHouseColumn(name, array)
            case .uint64:
                guard let array: [UInt64?] = readOptionalIntegerArray(numRows: numRows) else {
                    return nil
                }
                return ClickHouseColumn(name, array)
            case .uuid:
                fatalError("Not supported")
            case .fixedString(let int):
                fatalError("Not supported")
            case .string:
                fatalError("Not supported")
            case .nullable(let clickHouseTypeName):
                fatalError("Not supported")
            }
        }
    }
}
