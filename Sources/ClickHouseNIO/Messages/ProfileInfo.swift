//
//  ProfileInfo.swift
//  ClickHouseNIO
//
//  Created by Patrick Zippenfenig on 2019-11-26.
//

import Foundation
import NIO

struct ProfileInfo {
    let rows: UInt64
    let blocks: UInt64
    let bytes: UInt64
    let applied_limit: Bool
    let rows_before_limit: UInt64
    let calculated_rows_before_limit: Bool
    
    init?(from buffer: inout ByteBuffer) {
        guard let rows = buffer.readVarInt64(),
            let blocks = buffer.readVarInt64(),
            let bytes = buffer.readVarInt64(),
            let applied_limit: Int8 = buffer.readInteger(),
            let rows_before_limit = buffer.readVarInt64(),
            let calculated_rows_before_limit: Int8 = buffer.readInteger() else {
            return nil
        }
        self.rows = rows
        self.blocks = blocks
        self.bytes = bytes
        self.applied_limit = applied_limit == 1
        self.rows_before_limit = rows_before_limit
        self.calculated_rows_before_limit = calculated_rows_before_limit == 1
    }
}
