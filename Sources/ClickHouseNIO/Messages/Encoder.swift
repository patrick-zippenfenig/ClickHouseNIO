//
//  Encoder.swift
//  CNIOAtomics
//
//  Created by Patrick Zippenfenig on 2019-11-30.
//

import Foundation
import NIO

/// Encodes ClickHouseCommand to ByteBuffer
final class ClickHouseMessageEncoder: MessageToByteEncoder {
    typealias OutboundIn = Command

    enum Command {
        case clientConnect(database: String, user: String, password: String)
        case query(sql: String, revision: UInt64)
        case data(data: [DataColumnWithType], revision: UInt64)
        case ping
    }

    func encode(data: Command, out: inout ByteBuffer) throws {
        switch data {
        case .clientConnect(let database, let user, let password):
            // print("Sending hello")
            // out.reserveCapacity(20)
            out.writeVarInt64(ClientCodes.Hello.rawValue)
            out.writeClickHouseString("ClickHouse client")
            out.writeVarInt64(ClickHouseConnection.DBMS_VERSION_MAJOR)
            out.writeVarInt64(ClickHouseConnection.DBMS_VERSION_MINOR)
            out.writeVarInt64(ClickHouseConnection.REVISION)
            out.writeClickHouseString(database)
            out.writeClickHouseString(user)
            out.writeClickHouseString(password)
            // print(out.debugDescription)

        case .query(let sql, let revision):
            // print("Sending query")
            out.writeVarInt64(ClientCodes.Query.rawValue)
            // query id
            out.writeClickHouseString("\(UInt64.random(in: UInt64.min..<UInt64.max))")
            if revision >= ClickHouseMessageDecoder.DBMS_MIN_REVISION_WITH_CLIENT_INFO {
                out.writeInteger(UInt8(1)) // query_kind
                out.writeClickHouseString("") // initial_user
                out.writeClickHouseString("") // initial_query_id
                out.writeClickHouseString("[::ffff:127.0.0.1]:0") // initial_address
                out.writeInteger(UInt8(1)) // iface_type

                out.writeClickHouseString("") // info.os_user
                out.writeClickHouseString("") // info.client_hostname
                out.writeClickHouseString("ClickHouse client") // client_name

                out.writeVarInt64(ClickHouseConnection.DBMS_VERSION_MAJOR)
                out.writeVarInt64(ClickHouseConnection.DBMS_VERSION_MINOR)
                out.writeVarInt64(ClickHouseConnection.REVISION)

                if revision >= ClickHouseMessageDecoder.DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
                    out.writeClickHouseString("") // quota_key
                }
            }

            out.writeClickHouseString("") // empty string is a marker of the end of settings
            out.writeVarInt64(Stages.Complete.rawValue)
            out.writeVarInt64(CompressionState.Disable.rawValue)
            out.writeClickHouseString(sql)

            DataMessage().addToBuffer(buffer: &out, revision: revision)
            // print("Sending query done")

        case .data(let data, let revision):
            // print("Uploading data")
            let datamessage = DataMessage(is_overflows: 0, bucket_num: -1, columns: data)
            datamessage.addToBuffer(buffer: &out, revision: revision)
            DataMessage().addToBuffer(buffer: &out, revision: revision)

        case .ping:
            out.writeVarInt64(ClientCodes.Ping.rawValue)
        }
    }
}
