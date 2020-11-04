//
//  Decoder.swift
//  ClickHouseNIO
//
//  Created by Patrick Zippenfenig on 2019-11-30.
//

import Foundation
import NIO

/// Handles incoming byte messages from ClickHouse and decodes to ClickHouseResult
final class ClickHouseMessageDecoder: ByteToMessageDecoder {
    static public let DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES         : UInt64 = 50264;
    static public let DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS   : UInt64 = 51554;
    static public let DBMS_MIN_REVISION_WITH_BLOCK_INFO               : UInt64 = 51903;
    static public let DBMS_MIN_REVISION_WITH_CLIENT_INFO              : UInt64 = 54032;
    static public let DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE          : UInt64 = 54058;
    static public let DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO : UInt64 = 54060;
    
    enum Result {
        case serverInfo(ServerInfo)
        case data(DataMessage)
        case exception(ExceptionMessage)
        case progress(Progress)
        case endOfStream
        case pong
        case profileInfo(ProfileInfo)
    }
    
    typealias InboundOut = Result
    
    /// needs to be here to decode data packages
    var revision = ClickHouseConnection.REVISION
    
    func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        //print("readableBytes \(buffer.readableBytes)")
        var copy = buffer
        
        guard let code = copy.readVarInt64() else {
            return .needMoreData
        }
        guard let codeEnum = ServerCodes(rawValue: code) else {
            print(buffer.debugDescription)
            print(buffer.readString(length: buffer.readableBytes)!)
            fatalError("Invalid Server code received from clickhouse")
        }
        //print("### \(codeEnum)")
        switch codeEnum {
        case .Hello:
            guard let info = ServerInfo(from: &copy) else {
                return .needMoreData
            }
            self.revision = info.revision
            buffer = copy
            assert(buffer.readableBytes == 0)
            context.fireChannelRead(wrapInboundOut(Result.serverInfo(info)))
            return .continue
        case .Data:
            guard let block = DataMessage(from: &copy, revision: revision) else {
                return .needMoreData
            }
            buffer = copy
            context.fireChannelRead(wrapInboundOut(Result.data(block)))
            return .continue
        case .Exception:
            guard let exception = ExceptionMessage(from: &copy) else {
                //print("need more data")
                return .needMoreData
            }
            buffer = copy
            context.fireChannelRead(wrapInboundOut(Result.exception(exception)))
            return .continue
        case .Progress:
            guard let progress = Progress(from: &copy, revision: revision) else {
                return .needMoreData
            }
            //print(progress)
            buffer = copy
            context.fireChannelRead(wrapInboundOut(Result.progress(progress)))
            return .continue
        case .Pong:
            buffer = copy
            context.fireChannelRead(wrapInboundOut(Result.pong))
            return .continue
        case .EndOfStream:
            buffer = copy
            assert(buffer.readableBytes == 0)
            context.fireChannelRead(wrapInboundOut(Result.endOfStream))
            return .continue
        case .ProfileInfo:
            guard let profileInfo = ProfileInfo(from: &copy) else {
                return .needMoreData
            }
            context.fireChannelRead(wrapInboundOut(Result.profileInfo(profileInfo)))
            buffer = copy
            return .continue
        case .Totals:
            //print(copy.debugDescription)
            //buffer = copy
            //return .continue
            fatalError("Message 'Totals' not yet impleemented")
        case .Extremes:
            fatalError("Message 'Extremes' not yet impleemented")
        }
    }
    
    func decodeLast(context: ChannelHandlerContext, buffer: inout ByteBuffer, seenEOF: Bool) throws -> DecodingState {
        //print("decodeLast", "seen EOF: \(seenEOF), buffer \(buffer)")
        // when sending a query with an error, we have to send a empty data packge
        // clickhouse will then send "Unexpected packet Data received from client"
        // therefore we have to clean the buffer. Otherwise there will be an endless loop
        //
        //print(buffer.readString(length: buffer.readableBytes)!)
        buffer.clear()
        //print(buffer.debugDescription)
        return .continue
    }
}
