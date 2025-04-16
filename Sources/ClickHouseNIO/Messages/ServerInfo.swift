//
//  ServerInfo.swift
//  ClickHouseNIO
//
//  Created by Patrick Zippenfenig on 2019-11-26.
//

import Foundation
import NIO

/// Response after a HELLO message
struct ServerInfo {
    let name: String
    let versionMajor: UInt64
    let versionMinor: UInt64
    let revision: UInt64
    let timezone: String?

    init?(from buffer: inout ByteBuffer) {
        guard let name = buffer.readClickHouseString(),
            let versionMajor = buffer.readVarInt64(),
            let versionMinor = buffer.readVarInt64(),
            let revision = buffer.readVarInt64() else {
                return nil
        }
        self.name = name
        self.versionMajor = versionMajor
        self.versionMinor = versionMinor
        self.revision = revision

        if revision >= ClickHouseMessageDecoder.DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
            guard let tz = buffer.readClickHouseString() else {
                return nil
            }
            self.timezone = tz
        } else {
            self.timezone = nil
        }
    }
}
