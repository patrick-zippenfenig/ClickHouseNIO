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

    public func merge(with: [Self]) throws -> Self {
        return Self(name, try values.merge(with: with.map({ $0.values })) )
    }
}

public protocol ClickHouseDataTypeArray {
    var count: Int { get }

    func merge(with: [ClickHouseDataTypeArray]) throws -> ClickHouseDataTypeArray

    func writeTo(buffer: inout ByteBuffer, type: ClickHouseTypeName, name: String)

    static func readFrom(buffer: inout ByteBuffer, numRows: Int, columnMetadata: ClickHouseColumnMetadata?) -> ClickHouseDataTypeArray?
}

extension Array: ClickHouseDataTypeArray where Element: ClickHouseDataType {
    public func merge(with: [ClickHouseDataTypeArray]) throws -> ClickHouseDataTypeArray {
        let sameType = try with.map { anyArray -> [Element] in
            guard let column = anyArray as? [Element] else {
                throw ClickHouseError.invalidDataType
            }
            return column
        }
        return ([self] + sameType).flatMap({ $0 })
    }

    public func writeTo(buffer: inout ByteBuffer, type: ClickHouseTypeName, name: String) {
        assert(type.string == Element.getClickHouseTypeName(columnMetadata: type.columnMetadata).string, "\(type.string), \(Element.getClickHouseTypeName(columnMetadata: type.columnMetadata).string)")
        buffer.writeClickHouseString(name)
        if type.string.hasPrefix("Enum") {
            buffer.writeClickHouseString(type.string.replacingOccurrences(of: "\": ", with: "' = ").replacingOccurrences(of: ", \"", with: ", '"))
        } else {
            buffer.writeClickHouseString(type.string)
        }
        Element.writeTo(buffer: &buffer, array: self, columnMetadata: type.columnMetadata)
    }

    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, columnMetadata: ClickHouseColumnMetadata?) -> ClickHouseDataTypeArray? {
        return Element.readFrom(buffer: &buffer, numRows: numRows, columnMetadata: columnMetadata)
    }
}

/// Container for the Column-Metadata
public enum ClickHouseColumnMetadata {
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
    case array(Self)
    case map(Self, Self)
    case nullable(Self)

    public init?(_ type: String) {
        if type.starts(with: "Nullable(") {
            let subTypeName = String(type.dropFirst("Nullable(".count).dropLast())
            guard let subType = Self(subTypeName) else {
                return nil
            }
            self = .nullable(subType)
        } else if type.starts(with: "FixedString(") {
            guard let len = Int(type.dropFirst("FixedString(".count).dropLast()) else {
                return nil
            }
            self = .fixedString(.fixedStringLength(len))
        } else if type.starts(with: "DateTime64(") {
            let stuff = type.dropFirst("DateTime64(".count).dropLast()
            if let coI = stuff.firstIndex(of: ",") {
                guard let precision = Int(stuff[stuff.startIndex..<coI]) else {
                    return nil
                }
                let timeZoneS = String(stuff[coI..<stuff.endIndex].dropFirst(2))
                self = .dateTime64(.dateTime64Precision(precision, timeZoneS))
            } else {
                guard let precision = Int(stuff) else {
                    return nil
                }
                self = .dateTime64(.dateTime64Precision(precision, nil))
            }
        } else if type.starts(with: "DateTime(") {
            let stuff = type.dropFirst("DateTime(".count).dropLast()
            let timeZoneS = String(stuff)
            self = .dateTime(.dateTimeTimeZone(timeZoneS))
        } else if type.starts(with: "Array(") {
            let subTypeName = String(type.dropFirst("Array(".count).dropLast())
            guard let subType = Self(subTypeName) else {
                return nil
            }
            self = .array(subType)
        } else if type.starts(with: "Map(") {
            let typeName = String(type.dropFirst("Map(".count).dropLast())
            let typeNames = typeName.split(separator: ",").map { $0.trimmingCharacters(in: .whitespaces) }
            guard typeNames.count == 2 else {
                return nil
            }
            guard
                let typeName = typeNames.first,
                let keyType = Self(typeName)
            else {
                return nil
            }
            guard
                let typeName = typeNames.last,
                let valueType = Self(typeName)
            else {
                return nil
            }
            self = .map(keyType, valueType)
        } else if type.starts(with: "Enum8(") {
            let columnMapping = String(type.dropFirst("Enum8(".count).dropLast())
            guard let data = ("{" + columnMapping.replacingOccurrences(of: "' = ", with: "\" : ").replacingOccurrences(of: "'", with: "\"") + "}").data(using: .utf8),
                let mapping = try? JSONDecoder().decode([String: Int8].self, from: data) else {
                    return nil
                }
            self = .enum8(.enum8Map(mapping))
        } else if type.starts(with: "Enum16(") {
            let columnMapping = String(type.dropFirst("Enum16(".count).dropLast())
            guard let data = ("{" + columnMapping.replacingOccurrences(of: "' = ", with: "\" : ").replacingOccurrences(of: "'", with: "\"") + "}").data(using: .utf8),
                let mapping = try? JSONDecoder().decode([String: Int16].self, from: data) else {
                    return nil
                }
            self = .enum16(.enum16Map(mapping))
        } else {
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
                self = .date
            case "Date32":
                self = .date32
            case "DateTime":
                self = .dateTime(.dateTimeTimeZone(nil))
            case "Bool":
                self = .boolean
            default:
                return nil
            }
        }
    }

    public var columnMetadata: ClickHouseColumnMetadata? {
        switch self {
        case .fixedString(let len):
            return len
        case .dateTime64(let precision):
            return precision
        case .enum8(let m):
            return m
        case .enum16(let m):
            return m
        case .array(let subtype):
            return subtype.columnMetadata
        case .nullable(let subtype):
            return subtype.columnMetadata
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
        case .fixedString(let lenOuter):
            guard case let .fixedStringLength(len) = lenOuter else {
                fatalError("fixed-length strings should have fixedStringLength-enum for column-metadata, not \(lenOuter)")
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
            // print("Array(\(subtype.string))")
            return "Array(\(subtype.string))"
        case .map(let keyType, let valueType):
            return "Map(\(keyType.string), \(valueType.string))"
        case .boolean:
            return "Bool"
        case .date:
            return "Date"
        case .date32:
            return "Date32"
        case .dateTime(let timezoneDataOuter):
            guard case let .dateTimeTimeZone(timezoneData) = timezoneDataOuter else {
                fatalError("dateTime should have dateTimeTimeZone-enum for column-metadata, not \(timezoneDataOuter)")
            }
            if let timezoneData {
                return "DateTime(\(timezoneData))"
            }
            return "DateTime"
        case .dateTime64(let precisionOuter):
            guard case let .dateTime64Precision(precision, timezoneData) = precisionOuter else {
                fatalError("dateTime64 should have dateTime64precision-enum for column-metadata, not \(precisionOuter)")
            }
            if let timezoneData {
                return "DateTime64(\(precision), \(timezoneData))"
            }
            return "DateTime64(\(precision))"
        case .enum16(let mappingOuter):
            guard case let .enum16Map(mapping) = mappingOuter else {
                fatalError("enum16 should have enum16Map-enum for column-metadata, not \(mappingOuter)")
            }
            let hm = mapping.map({
                "'\($0.key)'=\($0.value)"
            }).joined(separator: ",")
            // "\(mapping)".replacingOccurrences(of: "[\"", with: "'")
            //     .replacingOccurrences(of: ",\"", with: ",'")
            //     .replacingOccurrences(of: "' : ", with: "' = ")
            //     .dropLast()
            return "Enum16(\(hm))"
        case .enum8(let mappingOuter):
            guard case let .enum8Map(mapping) = mappingOuter else {
                fatalError("enum8 should have enum8Map-enum for column-metadata, not \(mappingOuter)")
            }
            // mapping.map({
            //     "'\($0.key)'=\($0.value)"
            // }).joined(separator: ",")
            let hm = mapping.map({
                "'\($0.key)'=\($0.value)"
            }).joined(separator: ",")
            // print(hm)
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
        case .fixedString:
            return [String].self
        case .string:
            return [String].self
        case .array(let type):
            switch type {
            case .float:
                return [[Float]].self
            case .float64:
                return [[Double]].self
            case .int8:
                return [[Int8]].self
            case .int16:
                return [[Int16]].self
            case .int32:
                return [[Int32]].self
            case .int64:
                return [[Int64]].self
            case .uint8:
                return [[UInt8]].self
            case .uint16:
                return [[UInt16]].self
            case .uint32:
                return [[UInt32]].self
            case .uint64:
                return [[UInt64]].self
            case .uuid:
                return [[UUID]].self
            case .fixedString:
                return [[String]].self
            case .string:
                return [[String]].self
            case .nullable:
                fatalError("no nullable in array (for now)")
            case .boolean:
                return [[Bool]].self
            case .date:
                return [[ClickHouseDate]].self
            case .date32:
                return [[ClickHouseDate32]].self
            case .dateTime:
                return [[ClickHouseDateTime]].self
            case .dateTime64:
                return [[ClickHouseDateTime64]].self
            case .enum16:
                return [[ClickHouseEnum16]].self
            case .enum8:
                return [[ClickHouseEnum8]].self
            case .array:
                fatalError("array cannot be nested (for now)")
            case .map:
                fatalError("map cannot be nested (for now)")
            }
            return [String].self
        case .map(let keyType, let valueType):
            switch (keyType, valueType) {
            case (.string, .string):
                return [[String: String]].self
            default:
                fatalError("Map(\(keyType.string), \(valueType.string)) not supported yet, only Map(String, String) are supported (for now)")
            }
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
            case .fixedString:
                return [String?].self
            case .string:
                return [String?].self
            case .array:
                fatalError("no array in nullable (for now)")
            case .map:
                fatalError("no map in nullable (for now)")
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
            case .nullable:
                fatalError("Nullable cannot be nested")
            }
        }
    }
}

public protocol ClickHouseDataType {
    /// Used by ORM to determinte the type of a given array
    static func getClickHouseTypeName(columnMetadata: ClickHouseColumnMetadata?) -> ClickHouseTypeName

    /// Write an array of this type to a bytebuffer
    static func writeTo(buffer: inout ByteBuffer, array: [Self], columnMetadata: ClickHouseColumnMetadata?)

    /// Return nil for more data. Moved buffer forward if sucessfull
    static func readFrom(buffer: inout ByteBuffer, numRows: Int, columnMetadata: ClickHouseColumnMetadata?) -> [Self]?

    /// Default value. Used for nullable values
    static var clickhouseDefault: Self { get }
}

/// Struct for the Date32-ClickHouse-Type, has a date-field which is a swift-date
public struct ClickHouseDate32: ClickHouseDataType, CustomStringConvertible {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, columnMetadata: ClickHouseColumnMetadata?) -> [Self]? {
        guard let intergerArray: [Int32] = buffer.readIntegerArray(numRows: numRows) else {
            return nil
        }
        return intergerArray.map { .init(date: Date(timeIntervalSince1970: Double(24 * 3600 * Int($0)))) }
    }

    /// constructs a ClickHouseDate32 with the given Date without changing anything
    public init(_ exact: Date) {
        self.date = exact
    }

    init(date: Date) {
        self.date = date
    }

    /// constructs a ClickHouseDate32 for the given Date while setting the hour, minute and second to 0
    /// if the setting fails, returns nil
    public init?(flooring: Date) {
        guard let date = Calendar.current.date(bySettingHour: 0, minute: 0, second: 0, of: flooring) else {
            return nil
        }
        self.date = date
    }

    public static var clickhouseDefault: Self {
        return .init(date: .init(timeIntervalSince1970: 0))
    }

    public static func writeTo(buffer: inout ByteBuffer, array: [Self], columnMetadata: ClickHouseColumnMetadata?) {
        let intergerArray = array.map {
            Int32($0.date.timeIntervalSince1970 / (24 * 3600))
        }
        buffer.writeIntegerArray(intergerArray)
    }

    public static func getClickHouseTypeName(columnMetadata: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .date32
    }

    public var date: Date

    public var description: String {
        "\(date)"
    }
}

/// Struct for the Date-ClickHouse-Type, has a date-field which is a swift-date
public struct ClickHouseDate: ClickHouseDataType, CustomStringConvertible {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, columnMetadata: ClickHouseColumnMetadata?) -> [Self]? {
        guard let intergerArray: [UInt16] = buffer.readIntegerArray(numRows: numRows) else {
            return nil
        }
        return intergerArray.map { .init(.init(timeIntervalSince1970: Double(24 * 3600 * Int($0)))) }
    }

    public static var clickhouseDefault: Self {
        return .init(.init(timeIntervalSince1970: 0))
    }

    public static func writeTo(buffer: inout ByteBuffer, array: [Self], columnMetadata: ClickHouseColumnMetadata?) {
        let intergerArray: [UInt16] = array.map {
            UInt16($0.date.timeIntervalSince1970 / (24 * 3600))
        }
        buffer.writeIntegerArray(intergerArray)
    }

    /// constructs a ClickHouseDate with the given Date without changing anything
    public init(_ exact: Date) {
        self.date = exact
    }

    init(date: Date) {
        self.date = date
    }

    /// constructs a ClickHouseDate for the given Date while setting the hour, minute and second to 0
    /// if the setting fails, returns nil
    public init?(flooring: Date) {
        guard let date = Calendar.current.date(bySettingHour: 0, minute: 0, second: 0, of: flooring) else {
            return nil
        }
        self.date = date
    }

    public static func getClickHouseTypeName(columnMetadata: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .date
    }

    public var date: Date

    public var description: String {
        "\(date)"
    }
}

/// Struct for the DateTime64-ClickHouse-Type, has a date-field which is a swift-date
/// When reading, all dates get coerced into the date-range 1900-01-01 00:00:00 to 2299-12-31 23:59:59
/// as ClickHouse does so too when outputting the DateTime64 in String-form
public struct ClickHouseDateTime64: ClickHouseDataType, CustomStringConvertible {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, columnMetadata columnMetadataOuter: ClickHouseColumnMetadata?) -> [Self]? {
        guard let columnMetadata = columnMetadataOuter,
        case let .dateTime64Precision(precision, _) = columnMetadata else {
            fatalError("dateTime64 should have dateTime64precision-enum for column-metadata, not \(String(describing: columnMetadataOuter))")
        }
        guard let intergerArray: [Int64] = buffer.readIntegerArray(numRows: numRows) else {
            return nil
        }
        return intergerArray.map {
            let precisionFactor = pow(10.0, -1.0 * Double(precision))
            let secondsSince1970 = Double($0) * precisionFactor
            guard secondsSince1970 >= -2_208_988_800.0 else {
                return .init(date: .init(timeIntervalSince1970: -2_208_988_800.0 ))
            }
            guard secondsSince1970 < 10_413_792_000.0 else {
                return .init(date: .init(timeIntervalSince1970: 10_413_791_999.9 ))
            }
            return .init(date: .init(timeIntervalSince1970: secondsSince1970 ))
        }
    }

    public static var clickhouseDefault: Self {
        return .init(date: .init(timeIntervalSince1970: 0))
    }

    public static func writeTo(buffer: inout ByteBuffer, array: [Self], columnMetadata columnMetadataOuter: ClickHouseColumnMetadata?) {
        guard let columnMetadata = columnMetadataOuter, case let .dateTime64Precision(precision, _) = columnMetadata else {
            fatalError("dateTime64 should have dateTime64precision-enum for column-metadata, not \(String(describing: columnMetadataOuter))")
        }
        let intergerArray = array.map({
            Int64($0.date.timeIntervalSince1970 * pow(10.0, Double(precision)))
        })
        buffer.writeIntegerArray(intergerArray)
    }

    public static func getClickHouseTypeName(columnMetadata: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .dateTime64(columnMetadata!)
    }

    public var date: Date

    public init(_ exact: Date) {
        self.date = exact
    }

    init(date: Date) {
        self.date = date
    }

    public var description: String {
        "\(date)"
    }
}

/// Struct for the DateTime-ClickHouse-Type, has a date-field which is a swift-date
public struct ClickHouseDateTime: ClickHouseDataType, CustomStringConvertible {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, columnMetadata: ClickHouseColumnMetadata?) -> [Self]? {
        guard let intergerArray: [UInt32] = buffer.readIntegerArray(numRows: numRows) else {
            return nil
        }
        return intergerArray.map { .init(date: .init(timeIntervalSince1970: Double($0))) }
    }

    public static var clickhouseDefault: Self {
        return .init(date: .init(timeIntervalSince1970: 0))
    }

    public static func writeTo(buffer: inout ByteBuffer, array: [Self], columnMetadata: ClickHouseColumnMetadata?) {
        let intergerArray = array.map {
            UInt32($0.date.timeIntervalSince1970)
        }
        buffer.writeIntegerArray(intergerArray)
    }

    public static func getClickHouseTypeName(columnMetadata: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        if let columnMetadata, case let .dateTimeTimeZone(timeZone) = columnMetadata {
            return .dateTime(.dateTimeTimeZone(timeZone))
        }
        return .dateTime(.dateTimeTimeZone(nil))
    }

    public var date: Date

    public init(_ exact: Date) {
        self.date = exact
    }

    init(date: Date) {
        self.date = date
    }

    public var description: String {
        "\(date)"
    }
}

/// Struct for an Enum based on an Int8, only has the word-field which is a String
/// and the string representation of the enum, it doesn't know the corresponding Int8
public struct ClickHouseEnum8: ClickHouseDataType, CustomStringConvertible {
    public var word: String
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, columnMetadata: ClickHouseColumnMetadata?) -> [Self]? {
        guard case let .enum8Map(mapping) = columnMetadata! else {
            fatalError("enum8 should have enum8Map-enum for column-metadata, not \(String(describing: columnMetadata))")
        }
        let reversedEnumMapping = [Int8: String](uniqueKeysWithValues: zip(mapping.values, mapping.keys))
        guard let intergerArray: [Int8] = buffer.readIntegerArray(numRows: numRows) else {
            return nil
        }
        return intergerArray.map { return Self(word: reversedEnumMapping[$0]!) }
    }

    public static var clickhouseDefault: Self {
        return .init(word: "")
    }

    public init(word: String) {
        self.word = word
    }

    public static func writeTo(buffer: inout ByteBuffer, array: [Self], columnMetadata: ClickHouseColumnMetadata?) {
        guard case let .enum8Map(mapping) = columnMetadata! else {
            fatalError("enum8 should have enum8Map-enum for column-metadata, not \(String(describing: columnMetadata))")
        }
        let intergerArray = array.map { mapping[$0.word]! }
        buffer.writeIntegerArray(intergerArray)
    }

    public static func getClickHouseTypeName(columnMetadata: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .enum8(columnMetadata!)
    }

    public var description: String {
        "\(word)"
    }
}
/// Struct for an Enum based on an Int16, only has the word-field which is a String
/// and the string representation of the enum, it doesn't know the corresponding Int16
public struct ClickHouseEnum16: ClickHouseDataType, CustomStringConvertible {
    public var word: String

    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, columnMetadata: ClickHouseColumnMetadata?) -> [Self]? {
        guard case let .enum16Map(mapping) = columnMetadata! else {
            fatalError("enum16 should have enum16Map-enum for column-metadata, not \(String(describing: columnMetadata))")
        }
        let reversedEnumMapping = [Int16: String](uniqueKeysWithValues: zip(mapping.values, mapping.keys))
        let intergerArray: [Int16]? = buffer.readIntegerArray(numRows: numRows)
        return intergerArray?.map({
            return Self(word: reversedEnumMapping[$0]!)
        })
    }

    public init(word: String) {
        self.word = word
    }

    public static var clickhouseDefault: Self {
        return .init(word: "")
    }

    public static func writeTo(buffer: inout ByteBuffer, array: [Self], columnMetadata: ClickHouseColumnMetadata?) {
        guard case let .enum16Map(mapping) = columnMetadata! else {
            fatalError("enum16 should have enum16Map-enum for column-metadata, not \(String(describing: columnMetadata))")
        }
        let intergerArray = array.map { mapping[$0.word]! }
        buffer.writeIntegerArray(intergerArray)
    }

    public static func getClickHouseTypeName(columnMetadata: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .enum16(columnMetadata!)
    }

    public var description: String {
        "\(word)"
    }
}

extension String: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, columnMetadata: ClickHouseColumnMetadata?) -> [Self]? {
        if let columnMetadata {
            guard case let .fixedStringLength(fixedLength) = columnMetadata else {
                fatalError("fixed-length strings should have fixedStringLength-enum for column-metadata, not \(columnMetadata)")
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

    public func clickhouseReadValue(buffer: inout ByteBuffer, columnMetadata: ClickHouseColumnMetadata?) -> String? {
        return buffer.readClickHouseString()
    }

    public static var clickhouseDefault: Self {
        return ""
    }

    public static func writeTo(buffer: inout ByteBuffer, array: [Self], columnMetadata: ClickHouseColumnMetadata?) {
        if let columnMetadata {
            guard case let .fixedStringLength(length) = columnMetadata else {
                fatalError("strings should have fixedStringLength-enum for fixed length strings or nothing for column-metadata, not \(columnMetadata)")
            }
            buffer.writeClickHouseFixedStrings(array, length: length)
        } else {
            buffer.writeClickHouseStrings(array)
        }
    }

    public static func getClickHouseTypeName(columnMetadata: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        if let len = columnMetadata {
            return .fixedString(len)
        }
        return .string
    }
}

extension Int8: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, columnMetadata: ClickHouseColumnMetadata?) -> [Self]? {
        return buffer.readIntegerArray(numRows: numRows)
    }

    public static var clickhouseDefault: Self {
        return 0
    }

    public static func writeTo(buffer: inout ByteBuffer, array: [Self], columnMetadata: ClickHouseColumnMetadata?) {
        buffer.writeIntegerArray(array)
    }

    public static func getClickHouseTypeName(columnMetadata: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .int8
    }
}

extension Bool: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, columnMetadata: ClickHouseColumnMetadata?) -> [Self]? {
        return buffer.readUnsafeGenericArray(numRows: numRows)
    }

    public static var clickhouseDefault: Self {
        return false
    }

    public static func writeTo(buffer: inout ByteBuffer, array: [Self], columnMetadata: ClickHouseColumnMetadata?) {
        _ = array.withUnsafeBytes {
            buffer.writeBytes($0)
        }
    }

    public static func getClickHouseTypeName(columnMetadata: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .boolean
    }
}

extension Int16: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, columnMetadata: ClickHouseColumnMetadata?) -> [Self]? {
        return buffer.readIntegerArray(numRows: numRows)
    }

    public static var clickhouseDefault: Self {
        return 0
    }

    public static func writeTo(buffer: inout ByteBuffer, array: [Self], columnMetadata: ClickHouseColumnMetadata?) {
        buffer.writeIntegerArray(array)
    }

    public static func getClickHouseTypeName(columnMetadata: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .int16
    }
}

extension Int32: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, columnMetadata: ClickHouseColumnMetadata?) -> [Self]? {
        return buffer.readIntegerArray(numRows: numRows)
    }

    public static var clickhouseDefault: Self {
        return 0
    }

    public static func writeTo(buffer: inout ByteBuffer, array: [Self], columnMetadata: ClickHouseColumnMetadata?) {
        buffer.writeIntegerArray(array)
    }

    public static func getClickHouseTypeName(columnMetadata: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .int32
    }
}

extension Int64: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, columnMetadata: ClickHouseColumnMetadata?) -> [Self]? {
        return buffer.readIntegerArray(numRows: numRows)
    }

    public static var clickhouseDefault: Self {
        return 0
    }

    public static func writeTo(buffer: inout ByteBuffer, array: [Self], columnMetadata: ClickHouseColumnMetadata?) {
        buffer.writeIntegerArray(array)
    }

    public static func getClickHouseTypeName(columnMetadata: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .int64
    }
}

extension UInt8: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, columnMetadata: ClickHouseColumnMetadata?) -> [Self]? {
        return buffer.readIntegerArray(numRows: numRows)
    }

    public static var clickhouseDefault: Self {
        return 0
    }

    public static func writeTo(buffer: inout ByteBuffer, array: [Self], columnMetadata: ClickHouseColumnMetadata?) {
        buffer.writeIntegerArray(array)
    }

    public static func getClickHouseTypeName(columnMetadata: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .uint8
    }
}

extension UInt16: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, columnMetadata: ClickHouseColumnMetadata?) -> [Self]? {
        return buffer.readIntegerArray(numRows: numRows)
    }

    public static var clickhouseDefault: Self {
        return 0
    }

    public static func writeTo(buffer: inout ByteBuffer, array: [Self], columnMetadata: ClickHouseColumnMetadata?) {
        buffer.writeIntegerArray(array)
    }

    public static func getClickHouseTypeName(columnMetadata: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .uint16
    }
}

extension UInt32: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, columnMetadata: ClickHouseColumnMetadata?) -> [Self]? {
        return buffer.readIntegerArray(numRows: numRows)
    }

    public static var clickhouseDefault: Self {
        return 0
    }

    public static func writeTo(buffer: inout ByteBuffer, array: [Self], columnMetadata: ClickHouseColumnMetadata?) {
        buffer.writeIntegerArray(array)
    }

    public static func getClickHouseTypeName(columnMetadata: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .uint32
    }
}

extension UInt64: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, columnMetadata: ClickHouseColumnMetadata?) -> [Self]? {
        return buffer.readIntegerArray(numRows: numRows)
    }

    public static var clickhouseDefault: Self {
        return 0
    }

    public static func writeTo(buffer: inout ByteBuffer, array: [Self], columnMetadata: ClickHouseColumnMetadata?) {
        buffer.writeIntegerArray(array)
    }

    public static func getClickHouseTypeName(columnMetadata: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .uint64
    }
}

extension Float: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, columnMetadata: ClickHouseColumnMetadata?) -> [Self]? {
        return buffer.readUnsafeGenericArray(numRows: numRows)
    }

    public static var clickhouseDefault: Self {
        return 0
    }

    public static func writeTo(buffer: inout ByteBuffer, array: [Self], columnMetadata: ClickHouseColumnMetadata?) {
        _ = array.withUnsafeBytes {
            buffer.writeBytes($0)
        }
    }

    public static func getClickHouseTypeName(columnMetadata: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .float
    }
}

extension Double: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, columnMetadata: ClickHouseColumnMetadata?) -> [Self]? {
        return buffer.readUnsafeGenericArray(numRows: numRows)
    }

    public static var clickhouseDefault: Self {
        return 0
    }

    public static func writeTo(buffer: inout ByteBuffer, array: [Self], columnMetadata: ClickHouseColumnMetadata?) {
        _ = array.withUnsafeBytes {
            buffer.writeBytes($0)
        }
    }

    public static func getClickHouseTypeName(columnMetadata: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .float64
    }
}

extension UUID: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, columnMetadata: ClickHouseColumnMetadata?) -> [Self]? {
        return buffer.readUuidArray(numRows: numRows, endianness: .little)
    }

    public static var clickhouseDefault: Self {
        return UUID(uuidString: "00000000-0000-0000-0000-000000000000")!
    }

    public static func writeTo(buffer: inout ByteBuffer, array: [Self], columnMetadata: ClickHouseColumnMetadata?) {
        buffer.writeUuidArray(array, endianness: .little)
    }

    public static func getClickHouseTypeName(columnMetadata: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .uuid
    }
}

extension Optional: ClickHouseDataType where Wrapped: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, columnMetadata: ClickHouseColumnMetadata?) -> [Self]? {
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

        guard let data = Wrapped.readFrom(buffer: &bufferCopy, numRows: numRows, columnMetadata: columnMetadata) else {
            return nil
        }
        let mapped = data.enumerated().map({
            isnull[$0.offset] ? nil : $0.element
        })
        buffer = bufferCopy
        return mapped
    }

    public static var clickhouseDefault: Self {
        return nil
    }

    public static func writeTo(buffer: inout ByteBuffer, array: [Wrapped?], columnMetadata: ClickHouseColumnMetadata?) {
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
        Wrapped.writeTo(buffer: &buffer, array: mapped, columnMetadata: columnMetadata)
    }

    public static func getClickHouseTypeName(columnMetadata: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .nullable(Wrapped.getClickHouseTypeName(columnMetadata: columnMetadata))
    }
}

extension Array: ClickHouseDataType where Element: ClickHouseDataType {
    public static func readFrom(buffer: inout ByteBuffer, numRows: Int, columnMetadata: ClickHouseColumnMetadata?) -> [[Element]]? {
        guard let offsets: [UInt64] = buffer.readIntegerArray(numRows: numRows) else {
            return nil
        }

        var array: [[Element]] = []
        array.reserveCapacity(numRows)
        var last = 0

        for i1 in offsets {
            guard let elements = Element.readFrom(buffer: &buffer, numRows: Int(i1) - last, columnMetadata: columnMetadata) else {
                return nil // need more data
            }

            array.append(elements)
            last = Int(i1)
        }

        return array
    }

    public static var clickhouseDefault: Self {
        return []
    }

    public static func writeTo(buffer: inout ByteBuffer, array: [Self], columnMetadata: ClickHouseColumnMetadata?) {
        var offsets: [UInt64] = []
        offsets.reserveCapacity(array.count)
        for elements in array {
            offsets.append((offsets.last ?? 0) + UInt64(elements.count))
        }
        buffer.writeIntegerArray(offsets)
        array.forEach {
            Element.writeTo(buffer: &buffer, array: $0, columnMetadata: columnMetadata)
        }
    }

    public static func getClickHouseTypeName(columnMetadata: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .array(Element.getClickHouseTypeName(columnMetadata: columnMetadata))
    }
}

extension Dictionary: ClickHouseDataType where Key: ClickHouseDataType, Value: ClickHouseDataType {
    public static func getClickHouseTypeName(columnMetadata: ClickHouseColumnMetadata?) -> ClickHouseTypeName {
        return .map(Key.getClickHouseTypeName(columnMetadata: columnMetadata), Value.getClickHouseTypeName(columnMetadata: columnMetadata))
    }

    public static func writeTo(buffer: inout NIOCore.ByteBuffer, array: [Dictionary<Key, Value>], columnMetadata: ClickHouseColumnMetadata?) {
        var offsets: [Int] = []
        offsets.reserveCapacity(array.count)
        var currentOffset = 0
        for elements in array {
            currentOffset += elements.count
            offsets.append(currentOffset)
        }
        buffer.writeIntegerArray(offsets)
        var keys = [Key]()
        var values = [Value]()
        keys.reserveCapacity(currentOffset)
        values.reserveCapacity(currentOffset)
        array.forEach {
            $0.forEach { key, value in
                keys.append(key)
                values.append(value)
            }
        }
        Key.writeTo(buffer: &buffer, array: keys, columnMetadata: columnMetadata)
        Value.writeTo(buffer: &buffer, array: values, columnMetadata: columnMetadata)
    }

    public static func readFrom(buffer: inout NIOCore.ByteBuffer, numRows: Int, columnMetadata: ClickHouseColumnMetadata?) -> [Dictionary<Key, Value>]? {
        guard let offsets: [Int] = buffer.readIntegerArray(numRows: numRows) else {
            return nil
        }
        var array: [Dictionary<Key, Value>] = []
        array.reserveCapacity(numRows)
        let total = offsets.last ?? 0
        guard let keys = Key.readFrom(buffer: &buffer, numRows: total, columnMetadata: columnMetadata) else {
            return nil // need more data
        }
        guard let values = Value.readFrom(buffer: &buffer, numRows: total, columnMetadata: columnMetadata) else {
            return nil // need more data
        }
        var last = 0
        for i in offsets {
            var map: [Key: Value] = [:]
            map.reserveCapacity(i - last)
            for (key, value) in zip(keys[last..<i], values[last..<i]) {
                map[key] = value
            }
            array.append(map)
            last = i
        }
        return array
    }

    public static var clickhouseDefault: [Key: Value] {
        [:]
    }
}
