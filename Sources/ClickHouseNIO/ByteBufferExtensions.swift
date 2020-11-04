//
//  ByteBufferExtensions.swift
//  ClickHouseNIO
//
//  Created by Patrick Zippenfenig on 2019-11-26.
//

import Foundation
import NIO

extension ByteBuffer {
    /// Clickhouse writes strings first with the length and then the actual data
    mutating func writeClickHouseString(_ string: String) {
        // We have to access the buffer here directly, because we need to know the byte length
        if let written = string.utf8.withContiguousStorageIfAvailable({ utf8Bytes -> Int in
            // If it is a continous UTF8 string
            writeVarInt64(UInt64(utf8Bytes.count))
            return self.setBytes(utf8Bytes, at: writerIndex)
        }) {
            moveWriterIndex(forwardBy: written)
            return
        }
        var string = string
        let written = string.withUTF8 { utf8Bytes -> Int in
            writeVarInt64(UInt64(utf8Bytes.count))
            return self.setBytes(utf8Bytes, at: writerIndex)
        }
        moveWriterIndex(forwardBy: written)
    }
    
    /// Write a string with a fixed length. Returns the number of bytes written.
    @discardableResult
    @inlinable
    public mutating func setString(_ string: String, at index: Int, maxLength length: Int) -> Int {
        if let written = string.utf8.withContiguousStorageIfAvailable({ utf8Bytes in
            // If it is a continous UTF8 string
            self.setBytes(utf8Bytes.prefix(length), at: writerIndex)
        }) {
            return written
        }
        var string = string
        let written = string.withUTF8 {
            self.setBytes($0.prefix(length), at: writerIndex)
        }
        return written
    }
    
    /// Write a fixed string and zero padd in case the string is too short
    mutating func writeClickHouseFixedString(_ string: String, length: Int) {
        // Carefull, needs to work with UTF8
        let written = setString(string, at: writerIndex, maxLength: length)
        if written < length {
            setRepeatingByte(0, count: length - written, at: writerIndex + written)
        }
        self.moveWriterIndex(forwardBy: length)
    }
    
    mutating func writeClickHouseStrings(_ strings: [String]) {
        let stringLen = strings.reduce(0, {$0 + $1.count})
        let offsetLen = strings.count * MemoryLayout<Int>.size
        reserveCapacity(writableBytes + stringLen + offsetLen)
        for string in strings {
            writeClickHouseString(string)
        }
    }
    
    mutating func writeClickHouseFixedStrings(_ strings: [String], length: Int) {
        reserveCapacity(writableBytes + length * strings.count)
        for string in strings {
            writeClickHouseFixedString(string, length: length)
        }
    }
    
    mutating func readClickHouseString() -> String? {
        guard let length = readVarInt64() else {
            return nil
        }
        if length > 0x00FFFFFF {
            assert(false)
            return nil
        }
        guard readableBytes >= length else {
            return nil
        }
        return readString(length: Int(length))
    }
    
    /// Fancy click house coding...
    mutating func writeVarInt64(_ frm: UInt64) {
        var value = frm
        var byte: UInt8 = 0
        for _ in 0...8 {
            byte = UInt8(value & 0x7F)
            if (value > 0x7F) {
                byte |= 0x80
            }
            writeInteger(byte)
            value >>= 7
            if (value == 0) {
                break
            }
        }
    }
    
    mutating func readVarInt64() -> UInt64? {
        var value: UInt64 = 0
        for i: UInt8 in 0...8 {
            guard let byte: UInt8 = readInteger() else {
                return nil
            }
            value |= UInt64(byte & 0x7F) << UInt64(7 * i)
            if ((byte & 0x80) == 0) {
                return value
            }
        }
        return nil
    }
    
    mutating func readClickHouseStrings(numRows: Int) -> [String]? {
        // TODO calculate the required bytes more efficiently
        
        var strings = [String]()
        strings.reserveCapacity(numRows)
        for _ in 0..<numRows {
            guard let string = readClickHouseString() else {
                return nil
            }
            strings.append(string)
        }
        return strings
    }
    
    mutating func readIntegerArray<T: FixedWidthInteger>(numRows: Int) -> [T]? {
        guard readableBytes >= MemoryLayout<T>.size * numRows else {
            return nil
        }
        var array = [T]()
        array.reserveCapacity(numRows)
        for _ in 0..<numRows {
            guard let value: T = readInteger(endianness: .little) else {
                return nil
            }
            array.append(value)
        }
        return array
    }
    
    /**
     Read bytes as a specific array type. The data type should be continuously stored in memory. E.g. Does not work with strings
     TODO: Ensure that this works for all types... endians might also be an issue
     */
    mutating func readUnsafeGenericArray<T>(numRows: Int) -> [T]? {
        guard readableBytes >= MemoryLayout<T>.size * numRows else {
            return nil
        }
        return [T](unsafeUninitializedCapacity: numRows) { (buffer, initializedCount) in
            let numBytes = readableBytesView.withUnsafeBytes({ $0.copyBytes(to: buffer)})
            assert(numBytes / MemoryLayout<T>.size == numRows)
            moveReaderIndex(forwardBy: numBytes)
            initializedCount = numRows
        }
    }
    
    mutating func writeIntegerArray<T: FixedWidthInteger>(_ array: [T]) {
        reserveCapacity(array.count * MemoryLayout<T>.size + writableBytes)
        for element in array {
            writeInteger(element, endianness: .little)
        }
    }
}
