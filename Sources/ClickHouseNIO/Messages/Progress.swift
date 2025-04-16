//
//  Progress.swift
//  ClickHouseNIO
//
//  Created by Patrick Zippenfenig on 2019-11-26.
//

import Foundation
import NIO

struct Progress {
    let rows: UInt64
    let bytes: UInt64
    let total_rows: UInt64?

    init?(from buffer: inout ByteBuffer, revision: UInt64) {
        guard let rows = buffer.readVarInt64(),
            let bytes = buffer.readVarInt64() else {
            return nil
        }
        self.rows = rows
        self.bytes = bytes
        if revision >= ClickHouseMessageDecoder.DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS {
            guard let total_rows = buffer.readVarInt64() else {
                return nil
            }
            self.total_rows = total_rows
        } else {
            self.total_rows = nil
        }
    }
}
