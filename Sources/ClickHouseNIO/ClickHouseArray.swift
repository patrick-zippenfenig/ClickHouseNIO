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
    public let values: [ClickHouseDataType]
    
    public init(_ name: String, _ values: [ClickHouseDataType]) {
        self.name = name
        self.values = values
    }
}

public enum ClickHouseTypeName {
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
    
    public init?(_ type: String) {
        if type.starts(with: "FixedString(") {
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
        }
    }
}

public protocol ClickHouseDataType {
    /// Used by ORM to determinte the type of a given array
    static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName
}
extension String: ClickHouseDataType {
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        if let len = fixedLength {
            return .fixedString(len)
        }
        return .string
    }
}
extension Int8: ClickHouseDataType {
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .int8
    }
}
extension Int16: ClickHouseDataType {
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .int16
    }
}
extension Int32: ClickHouseDataType {
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .int32
    }
}
extension Int64: ClickHouseDataType {
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .int64
    }
}
extension UInt8: ClickHouseDataType {
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .uint8
    }
}
extension UInt16: ClickHouseDataType {
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .uint16
    }
}
extension UInt32: ClickHouseDataType {
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .uint32
    }
}
extension UInt64: ClickHouseDataType {
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .uint64
    }
}
extension Float: ClickHouseDataType {
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .float
    }
}
extension Double: ClickHouseDataType {
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .float64
    }
}
extension UUID: ClickHouseDataType {
    public static func getClickHouseTypeName(fixedLength: Int?) -> ClickHouseTypeName {
        return .uuid
    }
}
    
extension ByteBuffer {
    mutating func loadFromClickHouseArray(array: [ClickHouseDataType], fixedLength: Int?) {
        if let array = array as? [Int8] {
            writeIntegerArray(array)
        } else if let array = array as? [Int16] {
            writeIntegerArray(array)
        } else if let array = array as? [Int32] {
            writeIntegerArray(array)
        } else if let array = array as? [Int64] {
            writeIntegerArray(array)
        } else if let array = array as? [UInt8] {
            writeIntegerArray(array)
        } else if let array = array as? [UInt16] {
            writeIntegerArray(array)
        } else if let array = array as? [UInt32] {
            writeIntegerArray(array)
        } else if let array = array as? [UInt64] {
            writeIntegerArray(array)
        } else if let array = array as? [Float] {
            let _ = array.withUnsafeBytes {
                writeBytes($0)
            }
        } else if let array = array as? [Double] {
            let _ = array.withUnsafeBytes {
                writeBytes($0)
            }
        } else if let array = array as? [UUID] {
            let _ = array.withUnsafeBytes {
                writeBytes($0)
            }
        } else if let length = fixedLength, let array = array as? [String] {
            writeClickHouseFixedStrings(array, length: length)
        } else if let array = array as? [String] {
            writeClickHouseStrings(array)
        } else {
            fatalError("Unkown datatype in addToBuffer")
        }
    }
    
    
    mutating func toClickHouseArray(type: ClickHouseTypeName, numRows: Int) -> [ClickHouseDataType]? {
        switch type {
        case .string:
            guard let strings = readClickHouseStrings(numRows: numRows) else {
                return nil
            }
            return strings
        case .int64:
            guard let array: [Int64] = readIntegerArray(numRows: numRows) else {
                return nil
            }
            return array
        case .int32:
            guard let array: [Int32] = readIntegerArray(numRows: numRows) else {
                return nil
            }
            return array
        case .uint64:
            guard let array: [UInt64] = readIntegerArray(numRows: numRows) else {
                return nil
            }
            return array
        case .uint8:
            guard let array: [UInt8] = readIntegerArray(numRows: numRows) else {
                return nil
            }
            return array
        case .float:
            guard let array: [Float] = readUnsafeGenericArray(numRows: numRows) else {
                return nil
            }
            return array
        case .uuid:
            guard let array: [UUID] = readUnsafeGenericArray(numRows: numRows) else {
                return nil
            }
            return array
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
            return strings
        case .float64:
            guard let array: [Double] = readUnsafeGenericArray(numRows: numRows) else {
                return nil
            }
            return array
        case .int8:
            guard let array: [Int8] = readIntegerArray(numRows: numRows) else {
                return nil
            }
            return array
        case .int16:
            guard let array: [Int16] = readIntegerArray(numRows: numRows) else {
                return nil
            }
            return array
        case .uint16:
            guard let array: [UInt16] = readIntegerArray(numRows: numRows) else {
                return nil
            }
            return array
        case .uint32:
            guard let array: [UInt32] = readIntegerArray(numRows: numRows) else {
                return nil
            }
            return array
        }
    }
}
