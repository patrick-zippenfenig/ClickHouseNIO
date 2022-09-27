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
    
    static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: ClickHouseColumnMetadata?) -> ClickHouseDataTypeArray?
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
        if type.string.hasPrefix("Enum") {
            buffer.writeClickHouseString(type.string.replacingOccurrences(of: "\": ", with: "' = ").replacingOccurrences(of: ", \"", with: ", '"))
        } else {
            buffer.writeClickHouseString(type.string)
        }
        Element.writeTo(buffer: &buffer, array: self, fixedLength: type.fixedLength)
    }
    
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: ClickHouseColumnMetadata?) -> ClickHouseDataTypeArray? {
        return Element.readFrom(buffer: &buffer, numRows: numRows, fixedLength: fixedLength)
    }
}

public enum ClickHouseColumnMetadata: Codable {
    case fixedStringLength(Int)
    case dateTimeTimeZone(String?)
    case dateTime64Precision(Int, String?)
    case enum8Map([String: Int8])
    case enum16Map([String: Int16])
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
    case fixedString(ClickHouseColumnMetadata)
    case string
    case enum8(ClickHouseColumnMetadata)
    case enum16(ClickHouseColumnMetadata)
    case boolean
    case date
    case date32
    case dateTime(ClickHouseColumnMetadata)
    case dateTime64(ClickHouseColumnMetadata)
    case array(ClickHouseTypeName)
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
            self = .fixedString(.fixedStringLength(len))
        }
        else if type.starts(with: "DateTime64(") {
            let stuff = type.dropFirst("DateTime64(".count).dropLast()
            if let coI = stuff.firstIndex(of: ",") {
                guard let precision = Int(stuff[stuff.startIndex..<coI]) else {
                    return nil
                }
                let timeZoneS = String(stuff[coI..<stuff.endIndex])
                self = .dateTime64(.dateTime64Precision(precision, timeZoneS))
            }
            else {
                guard let precision = Int(stuff) else {
                    return nil
                }    
                self = .dateTime64(.dateTime64Precision(precision, nil))
            }
        }
        else if type.starts(with: "DateTime(") {
            let stuff = type.dropFirst("DateTime(".count).dropLast()
            let timeZoneS = String(stuff)
            self = .dateTime64(.dateTimeTimeZone(timeZoneS))
            
        }
        else if type.starts(with: "Array(") {
            let subTypeName = String(type.dropFirst("Array(".count).dropLast())
            guard let subType = ClickHouseTypeName(subTypeName) else {
                return nil
            }
            self = .array(subType)
        }
        else if type.starts(with: "Enum8(") {
            let subTypeName = String(type.dropFirst("Enum8(".count).dropLast())            
            let s = "{" + subTypeName.replacingOccurrences(of: "' = ", with: "\" : ").replacingOccurrences(of: "'", with: "\"") + "}"
            let da = s.data(using: .utf8)!
            let su = (try? JSONDecoder().decode([String: Int8].self, from: da))!
            self = .enum8(.enum8Map(su)) 
        }
        else if type.starts(with: "Enum16(") {
            let subTypeName = String(type.dropFirst("Enum16(".count).dropLast())            
            let s = "{" + subTypeName.replacingOccurrences(of: "' = ", with: "\" : ").replacingOccurrences(of: "'", with: "\"") + "}"
            let da = s.data(using: .utf8)!
            let su = (try? JSONDecoder().decode([String: Int16].self, from: da))!
            self = .enum16(.enum16Map(su)) 
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
            case "Date":
                self =  .date
            case "Date32":
                self  = .date32
            case "DateTime":
                self  = .dateTime(.dateTimeTimeZone(nil))
            case "Bool":
                self  = .boolean
            default:
                return nil
            }
        }
    }
    public var fixedLength: ClickHouseColumnMetadata? {
        switch self {
        case .fixedString(let len):
            return len
        case .dateTime64(let precision):
            return precision
        case .enum8(let m):
            return m
        case .enum16(let m):
            return m
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
            guard case let .fixedStringLength(len) = len else {
                fatalError()
            }
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
        case .array(let subtype):
            return "Array(\(subtype.string))"
        case .boolean: 
            return "Bool"
        case .date: 
            return "Date"
        case .date32: 
            return "Date32"
        case .dateTime(let timezone): 
            guard case let .dateTimeTimeZone(timezone) = timezone else {
                fatalError()
            }
            if let timezone = timezone {
                return "DateTime64(\(timezone))"

            }
            return "DateTime"
        case .dateTime64(let precision): 
            guard case let .dateTime64Precision(precision, timezone) = precision else {
                fatalError()
            }
            if let timezone = timezone {
                return "DateTime64(\(precision), \(timezone))"

            }
            return "DateTime64(\(precision))"
        case .enum16(let mapping): 
            guard case let .enum16Map(mapping) = mapping else {
                fatalError("is type: \(mapping)")
            }
            let hm = "\(mapping)".replacingOccurrences(of: "[\"", with: "'")
                .replacingOccurrences(of: ",\"", with: ",'")
                .replacingOccurrences(of: "' : ", with: "' = ")
                .dropLast()
            return "Enum16(\(hm))"
        case .enum8(let mapping): 
            guard case let .enum8Map(mapping) = mapping else {
                fatalError()
            }
            let hm = "\(mapping)".replacingOccurrences(of: "[\"", with: "'")
                .replacingOccurrences(of: ",\"", with: ",'")
                .replacingOccurrences(of: "' : ", with: "' = ")
                .dropLast()
            return "Enum8(\(hm))"
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
        case .array(let type):
            switch type {
            case .float:
                return [Array<Float>].self
            case .float64:
                return [Array<Double>].self
            case .int8:
                return [Array<Int8>].self
            case .int16:
                return [Array<Int16>].self
            case .int32:
                return [Array<Int32>].self
            case .int64:
                return [Array<Int64>].self
            case .uint8:
                return [Array<UInt8>].self
            case .uint16:
                return [Array<UInt16>].self
            case .uint32:
                return [Array<UInt32>].self
            case .uint64:
                return [Array<UInt64>].self
            case .uuid:
                return [Array<UUID>].self
            case .fixedString(_):
                return [Array<String>].self
            case .string:
                return [Array<String>].self
            case .nullable(_):
                fatalError("no nullable in array")
            case .boolean: 
                return [Array<Bool>].self
            case .date: 
                return [Array<ClickHouseDate>].self
            case .date32: 
                return [Array<ClickHouseDate32>].self
            case .dateTime: 
                return [Array<ClickHouseDateTime>].self
            case .dateTime64: 
                return [Array<ClickHouseDateTime64>].self
            case .enum16: 
                return [Array<ClickHouseEnum16>].self
            case .enum8: 
                return [Array<ClickHouseEnum8>].self
            case .array(_):
                fatalError("array cannot be nested (for now)")
            }
            return [String].self
        case .boolean: 
            return [Bool].self
        case .date: 
            return [ClickHouseDate].self
        case .date32: 
            return [ClickHouseDate32].self
        case .dateTime: 
            return [ClickHouseDateTime].self
        case .dateTime64: 
            return [ClickHouseDateTime64].self
        case .enum16: 
            return [ClickHouseEnum16].self
        case .enum8: 
            return [ClickHouseEnum8].self
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
            case .array(_):
                fatalError("no array in nullable")
            case .boolean: 
                return [Bool?].self
            case .date: 
                return [ClickHouseDate?].self
            case .date32: 
                return [ClickHouseDate32?].self
            case .dateTime: 
                return [ClickHouseDateTime?].self
            case .dateTime64: 
                return [ClickHouseDateTime64?].self
            case .enum16: 
                return [ClickHouseEnum16?].self
            case .enum8: 
                return [ClickHouseEnum8?].self
            case .nullable(_):
                fatalError("Nullable cannot be nested")
            }
        }
    }
}

public protocol ClickHouseDataType {
    /// Used by ORM to determinte the type of a given array
    static func getClickHouseTypeName(fixedLength: ClickHouseColumnMetadata?) -> ClickHouseTypeName
    
    /// Write an array of this type to a bytebuffer
    static func writeTo(buffer: inout ByteBuffer, array: [Self], fixedLength: ClickHouseColumnMetadata?)
    
    /// Return nil for more data. Moved buffer forward if sucessfull
    static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: ClickHouseColumnMetadata?) -> [Self]?
    
    /// Default value. Used for nullable values
    static var clickhouseDefault: Self { get }
}

public struct ClickHouseDate32: ClickHouseDataType, CustomStringConvertible {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: ClickHouseColumnMetadata?) -> [ClickHouseDate32]? {
        guard let a: [Int32] = buffer.readIntegerArray(numRows: numRows) else {
            return nil
        }
        return a.map {
            .init(_date: 
                Date(timeIntervalSince1970: Double(24 * 3600 * Int($0)))
            )
        }
    }

    public init(_ exact: Date) {
        self._date = exact
    }
    
    init(_date: Date) {
        self._date = _date
    }
    
    public static var clickhouseDefault: ClickHouseDate32 {
        return .init(_date: .init(timeIntervalSince1970: 0))
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [ClickHouseDate32], fixedLength: ClickHouseColumnMetadata?) {
        let a = array.map {
            Int32($0._date.timeIntervalSince1970 / (24 * 3600))
        }
        buffer.writeIntegerArray(a)
    }
    
    public static func getClickHouseTypeName(fixedLength: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .date32
    }

    public var _date: Date

    public var description: String {
        "\(_date)"
    }   
}

public struct ClickHouseDate: ClickHouseDataType, CustomStringConvertible {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: ClickHouseColumnMetadata?) -> [ClickHouseDate]? {
        guard let a: [UInt16] = buffer.readIntegerArray(numRows: numRows) else {
            return nil
        }
        return a.map {
            .init(.init(timeIntervalSince1970: Double(24 * 3600 * Int($0))))
        }
    }
    
    public static var clickhouseDefault: ClickHouseDate {
        return .init(.init(timeIntervalSince1970: 0))
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [ClickHouseDate], fixedLength: ClickHouseColumnMetadata?) {
        let a: [UInt16] = array.map {
            UInt16($0._date.timeIntervalSince1970 / (24 * 3600))
        }
        buffer.writeIntegerArray(a)
    }

    // public init(rounding: Date) {
        
    // }
    public init?(flooring: Date) {
        guard let _date = Calendar.current.date(bySettingHour: 0, minute: 0, second: 0, of: flooring) else {
            return nil
        }
        self._date = _date
    }
    // public init(ceeling: Date) {

    // }
    public init(_ exact: Date) {
        self._date = exact
    }

    public static func getClickHouseTypeName(fixedLength: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .date
    }

    public var _date: Date

    public var description: String {
        "\(_date)"
    }   
}
public struct ClickHouseDateTime64: ClickHouseDataType, CustomStringConvertible {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: ClickHouseColumnMetadata?) -> [ClickHouseDateTime64]? {
        guard case let .dateTime64Precision(precision, _) = fixedLength! else {
            fatalError()
        }
        let hm: [Int64]? = buffer.readUnsafeGenericArray(numRows: numRows)
        return hm?.map({
            .init(_date: .init(timeIntervalSince1970: (
                Double($0) / pow(10.0, Double(precision))
            )))
        })
    }
    
    public static var clickhouseDefault: ClickHouseDateTime64 {
        return .init(_date: .init(timeIntervalSince1970: 0))
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [ClickHouseDateTime64], fixedLength: ClickHouseColumnMetadata?) {
        guard case let .dateTime64Precision(precision, _) = fixedLength! else {
            fatalError()
        }
        let hm = array.map({
            Int64($0._date.timeIntervalSince1970 * pow(10.0, Double(precision)))
        })
        let _ = hm.withUnsafeBytes {
            buffer.writeBytes($0)
        }
    }
    
    public static func getClickHouseTypeName(fixedLength: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .dateTime64(fixedLength!)
    }

    public var _date: Date

    public init(_ exact: Date) {
        self._date = exact
    }
    
    init(_date: Date) {
        self._date = _date
    }

    public var description: String {
        "\(_date)"
    }   
}

public struct ClickHouseDateTime: ClickHouseDataType, CustomStringConvertible {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: ClickHouseColumnMetadata?) -> [ClickHouseDateTime]? {
        guard let a: [UInt32] = buffer.readIntegerArray(numRows: numRows) else {
            return nil
        }
        return a.map {
            .init(_date: .init(timeIntervalSince1970: Double($0)))
        }
    }
    
    public static var clickhouseDefault: ClickHouseDateTime {
        return .init(_date: .init(timeIntervalSince1970: 0))
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [ClickHouseDateTime], fixedLength: ClickHouseColumnMetadata?) {
        let a = array.map {
            UInt32($0._date.timeIntervalSince1970)
        }
        buffer.writeIntegerArray(a)
    }
    
    public static func getClickHouseTypeName(fixedLength: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .dateTime(.dateTimeTimeZone(nil))
    }

    public var _date: Date

    public init(_ exact: Date) {
        self._date = exact
    }
    
    init(_date: Date) {
        self._date = _date
    }


    public var description: String {
        "\(_date)"
    }   
}

public struct ClickHouseEnum8: ClickHouseDataType, CustomStringConvertible {
    public var __str: String
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: ClickHouseColumnMetadata?) -> [ClickHouseEnum8]? {
        guard case let .enum8Map(mapping) = fixedLength! else {
            fatalError()
        }
        let ma2 = [Int8: String](uniqueKeysWithValues: zip(mapping.values, mapping.keys))
        let hm: [Int8]? = buffer.readIntegerArray(numRows: numRows)
        return hm?.map({
            return ClickHouseEnum8(__str: ma2[$0]!)
        })
    }
    
    public static var clickhouseDefault: ClickHouseEnum8 {
        return .init(__str: "")
    }

    public init(__str: String) {
        self.__str = __str
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [ClickHouseEnum8], fixedLength: ClickHouseColumnMetadata?) {
        guard case let .enum8Map(mapping) = fixedLength! else {
            fatalError()
        }
        let hm = array.map { mapping[$0.__str]! }
        print(hm)
        buffer.writeIntegerArray(hm)
        // let _ = hm.withUnsafeBytes {
        //     buffer.writeBytes($0)
        // }
    }
    
    public static func getClickHouseTypeName(fixedLength: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .enum8(fixedLength!)
    }

    public var description: String {
        "\(__str)"
    }
}

public struct ClickHouseEnum16: ClickHouseDataType, CustomStringConvertible {
    public var __str: String
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: ClickHouseColumnMetadata?) -> [ClickHouseEnum16]? {
        guard case let .enum16Map(mapping) = fixedLength! else {
            fatalError()
        }
        let ma2 = [Int16: String](uniqueKeysWithValues: zip(mapping.values, mapping.keys))
        let hm: [Int16]? = buffer.readIntegerArray(numRows: numRows)
        return hm?.map({
            return ClickHouseEnum16(__str: ma2[$0]!)
        })
    }

    public init(__str: String) {
        self.__str = __str
    }
    
    public static var clickhouseDefault: ClickHouseEnum16 {
        return .init(__str: "")
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [ClickHouseEnum16], fixedLength: ClickHouseColumnMetadata?) {
        guard case let .enum16Map(mapping) = fixedLength! else {
            fatalError()
        }
        let hm = array.map { mapping[$0.__str]! }
        print(hm)
        buffer.writeIntegerArray(hm)
    }
    
    public static func getClickHouseTypeName(fixedLength: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .enum16(fixedLength!)
    }

    public var description: String {
        "\(__str)"
    }
}

extension String: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: ClickHouseColumnMetadata?) -> [String]? {
        
        if let fixedLength = fixedLength {
            guard case let .fixedStringLength(fixedLength) = fixedLength else {
                fatalError()
            }
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
    
    public func clickhouseReadValue(buffer: inout ByteBuffer, fixedLength: ClickHouseColumnMetadata?) -> String? {
        return buffer.readClickHouseString()
    }
    
    public static var clickhouseDefault: String {
        return ""
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [String], fixedLength: ClickHouseColumnMetadata?) {
        if let length = fixedLength {
            guard case let .fixedStringLength(length) = length else {
                fatalError()
            }
            buffer.writeClickHouseFixedStrings(array, length: length)
        } else {
            buffer.writeClickHouseStrings(array)
        }
    }
    
    public static func getClickHouseTypeName(fixedLength: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        if let len = fixedLength {
            return .fixedString(len)
        }
        return .string
    }
}

extension Int8: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: ClickHouseColumnMetadata?) -> [Int8]? {
        return buffer.readIntegerArray(numRows: numRows)
    }
    
    public static var clickhouseDefault: Int8 {
        return 0
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [Int8], fixedLength: ClickHouseColumnMetadata?) {
        buffer.writeIntegerArray(array)
    }
    
    public static func getClickHouseTypeName(fixedLength: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .int8
    }
}

extension Bool: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: ClickHouseColumnMetadata?) -> [Bool]? {
        return buffer.readUnsafeGenericArray(numRows: numRows)
    }
    
    public static var clickhouseDefault: Bool {
        return false
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [Bool], fixedLength: ClickHouseColumnMetadata?) {
        let _ = array.withUnsafeBytes {
            buffer.writeBytes($0)
        }
    }
    
    public static func getClickHouseTypeName(fixedLength: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .boolean
    }
}

extension Int16: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: ClickHouseColumnMetadata?) -> [Int16]? {
        return buffer.readIntegerArray(numRows: numRows)
    }
    
    public static var clickhouseDefault: Int16 {
        return 0
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [Int16], fixedLength: ClickHouseColumnMetadata?) {
        buffer.writeIntegerArray(array)
    }
    
    public static func getClickHouseTypeName(fixedLength: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .int16
    }
}

extension Int32: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: ClickHouseColumnMetadata?) -> [Int32]? {
        return buffer.readIntegerArray(numRows: numRows)
    }
    
    public static var clickhouseDefault: Int32 {
        return 0
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [Int32], fixedLength: ClickHouseColumnMetadata?) {
        buffer.writeIntegerArray(array)
    }
    
    public static func getClickHouseTypeName(fixedLength: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .int32
    }
}

extension Int64: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: ClickHouseColumnMetadata?) -> [Int64]? {
        return buffer.readIntegerArray(numRows: numRows)
    }
    
    public static var clickhouseDefault: Int64 {
        return 0
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [Int64], fixedLength: ClickHouseColumnMetadata?) {
        buffer.writeIntegerArray(array)
    }
    
    public static func getClickHouseTypeName(fixedLength: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .int64
    }
}

extension UInt8: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: ClickHouseColumnMetadata?) -> [UInt8]? {
        return buffer.readIntegerArray(numRows: numRows)
    }
    
    public static var clickhouseDefault: UInt8 {
        return 0
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [UInt8], fixedLength: ClickHouseColumnMetadata?) {
        buffer.writeIntegerArray(array)
    }
    
    public static func getClickHouseTypeName(fixedLength: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .uint8
    }
}

extension UInt16: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: ClickHouseColumnMetadata?) -> [UInt16]? {
        return buffer.readIntegerArray(numRows: numRows)
    }
    
    public static var clickhouseDefault: UInt16 {
        return 0
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [UInt16], fixedLength: ClickHouseColumnMetadata?) {
        buffer.writeIntegerArray(array)
    }
    
    public static func getClickHouseTypeName(fixedLength: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .uint16
    }
}

extension UInt32: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: ClickHouseColumnMetadata?) -> [UInt32]? {
        return buffer.readIntegerArray(numRows: numRows)
    }
    
    public static var clickhouseDefault: UInt32 {
        return 0
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [UInt32], fixedLength: ClickHouseColumnMetadata?) {
        buffer.writeIntegerArray(array)
    }
    
    public static func getClickHouseTypeName(fixedLength: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .uint32
    }
}

extension UInt64: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: ClickHouseColumnMetadata?) -> [UInt64]? {
        return buffer.readIntegerArray(numRows: numRows)
    }
    
    public static var clickhouseDefault: UInt64 {
        return 0
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [UInt64], fixedLength: ClickHouseColumnMetadata?) {
        buffer.writeIntegerArray(array)
    }
    
    public static func getClickHouseTypeName(fixedLength: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .uint64
    }
}

extension Float: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: ClickHouseColumnMetadata?) -> [Float]? {
        return buffer.readUnsafeGenericArray(numRows: numRows)
    }
    
    public static var clickhouseDefault: Float {
        return 0
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [Float], fixedLength: ClickHouseColumnMetadata?) {
        let _ = array.withUnsafeBytes {
            buffer.writeBytes($0)
        }
    }
    
    public static func getClickHouseTypeName(fixedLength: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .float
    }
}

extension Double: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: ClickHouseColumnMetadata?) -> [Double]? {
        return buffer.readUnsafeGenericArray(numRows: numRows)
    }
    
    public static var clickhouseDefault: Double {
        return 0
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [Double], fixedLength: ClickHouseColumnMetadata?) {
        let _ = array.withUnsafeBytes {
            buffer.writeBytes($0)
        }
    }
    
    public static func getClickHouseTypeName(fixedLength: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .float64
    }
}

extension UUID: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: ClickHouseColumnMetadata?) -> [UUID]? {
        return buffer.readUuidArray(numRows: numRows, endianness: .little)
    }
    
    public static var clickhouseDefault: UUID {
        return UUID(uuidString: "00000000-0000-0000-0000-000000000000")!
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [UUID], fixedLength: ClickHouseColumnMetadata?) {
        buffer.writeUuidArray(array, endianness: .little)
    }
    
    public static func getClickHouseTypeName(fixedLength: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .uuid
    }
}

extension Optional: ClickHouseDataType where Wrapped: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: ClickHouseColumnMetadata?) -> [Optional<Wrapped>]? {
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
    
    public static func writeTo(buffer: inout ByteBuffer, array: [Optional<Wrapped>], fixedLength: ClickHouseColumnMetadata?) {
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
    
    public static func getClickHouseTypeName(fixedLength: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .nullable(Wrapped.getClickHouseTypeName(fixedLength: fixedLength))
    }
}

extension Array: ClickHouseDataType where Element: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, fixedLength: ClickHouseColumnMetadata?) -> [[Element]]? {
        guard numRows > 0 else {
            return  []
        }
        var bufferCopy = buffer
        
        guard bufferCopy.readableBytes >= (1) * numRows else {
            return nil
        }
        var offsets = [UInt64]()
        offsets.reserveCapacity(numRows)
        for _ in 0..<(numRows) {
            guard let set: UInt64 = bufferCopy.readInteger(endianness: .little) else {
                return nil // need more data
            }
            offsets.append(set)
        }
        var array: [[Element]] = []
        array.reserveCapacity(numRows)
        var last = 0
        for i1 in offsets {
            guard let ele = Element.readFrom(buffer: &bufferCopy, numRows: Int(i1) - last, fixedLength: fixedLength) else {
                return nil // need more data
            }
            array.append(ele)
            last = Int(i1)
        }
        buffer = bufferCopy
        return array
    }
    
    public static var clickhouseDefault: [Element] {
        return []
    }
    
    public static func writeTo(buffer: inout ByteBuffer, array: [[Element]], fixedLength: ClickHouseColumnMetadata?) {
       var offsets: [UInt64] = []
        offsets.reserveCapacity(array.count)
        for a in array {
            offsets.append((offsets.last ?? 0) + UInt64(a.count))
        }
        buffer.writeIntegerArray(offsets)
        array.forEach({
            Element.writeTo(buffer: &buffer, array: $0, fixedLength: fixedLength)
        })
    }
    
    public static func getClickHouseTypeName(fixedLength: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .array(Element.getClickHouseTypeName(fixedLength: fixedLength))
    }
}