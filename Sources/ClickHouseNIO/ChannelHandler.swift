//
//  ChannelHandler.swift
//  ClickHouseNIO
//
//  Created by Patrick Zippenfenig on 27.10.20.
//

import Foundation
import Logging
import NIO

enum ClickHouseCommand {
    case clientConnect(database: String, user: String, password: String)
    case query(sql: String)
    case command(sql: String)
    case insert(table: String, data: [ClickHouseColumn])
    case ping
}

enum ClickHouseResult {
    case serverInfo(ServerInfo)
    case error(ExceptionMessage)
    case result(ClickHouseQueryResult)
    case queryExecuted
    case pong
}

public struct ClickHouseQueryResult {
    public let columns: [ClickHouseColumn]

    /// ClickHouse transmits multiple DataMessages for each query. Usually the first has all columns, but no data. Here we merge them together.
    fileprivate init(messages: [DataMessage]) {
        assert(!messages.isEmpty)
        assert(messages.first?.rowCount == 0)
        assert(!messages.contains(where: { $0.columnCount != messages[0].columnCount }))
        if messages.isEmpty {
            columns = []
            return
        }

        /// Commands like drop table, only return one message with the tables
        if messages.count == 1 {
            columns = messages[0].columns.map { $0.column }
            return
        }

        /// If we only have 2 messages and the first has no data, we can return the same array
        if messages.count == 2 && messages[0].rowCount == 0 {
            columns = messages[1].columns.map { $0.column }
            return
        }
        columns = (0 ..< messages[0].columnCount).map { i in
            let c = messages.map { $0.columns[i].column }
            guard let first = c.first else {
                fatalError("Could not construct 'ClickHouseQueryResult'")
            }
            // only fails on merging different types
            // swiftlint:disable:next force_try
            return try! first.merge(with: Array(c.dropFirst()))
        }
    }
}

enum ClickHouseError: Error {
    case expectedPong
    case expectedServerInfo
    case queryDidNotReturnAnyDataPleaseUseCommand
    case commandReturnedDataPleaseUseQuery
    case expectedConnectCommand
    case receivedDataButNotConnected
    case receivedDataButDidNotRequestAnything
    case expectedEndOfStream
    case connectionIsNotReadyToSendNewCommands
    case alreadyConnected
    case readTimeout
    case queryTimeout
    case invalidDataType
}

final class ClickHouseChannelHandler: ChannelDuplexHandler {
    public typealias InboundIn = ClickHouseMessageDecoder.Result
    public typealias InboundOut = ClickHouseResult

    public typealias OutboundIn = ClickHouseCommand
    public typealias OutboundOut = ClickHouseMessageEncoder.Command

    enum State {
        case notConnected
        case connecting
        case ready
        case awaitingQueryResult(blocks: [DataMessage])
        case awaitingQueryResultEndOfStream(result: ClickHouseQueryResult)
        case awaitingToSendData(data: [ClickHouseColumn])
        case awaitingPong
        case awaitingQueryConfirmation
        case closed
    }

    var state = State.notConnected

    var revision = ClickHouseConnection.REVISION

    let logger: Logger

    init(logger: Logger) {
        self.logger = logger
    }

    /// Data message FROM clickhouse server
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let response = self.unwrapInboundIn(data)
        logger.trace("Received message from clickhouse \(response)")

        if case .closed = state {
            // If the channel is closed, ignore any reads even errors.
            // This can happen, if a timeout is reached, the channel is closed, but clickhouse server sends an EOF exception
            return
        }

        if case .exception(let error) = response {
            state = .ready
            context.fireChannelRead(wrapInboundOut(ClickHouseResult.error(error)))
            return
        }

        switch state {
        case .closed, .notConnected:
            context.fireErrorCaught(ClickHouseError.receivedDataButNotConnected)
        case .connecting:
            guard case .serverInfo(let info) = response else {
                context.fireErrorCaught(ClickHouseError.expectedServerInfo)
                return
            }
            state = .ready
            revision = info.revision
            context.fireChannelRead(wrapInboundOut(ClickHouseResult.serverInfo(info)))
        case .ready:
            context.fireErrorCaught(ClickHouseError.receivedDataButDidNotRequestAnything)
        case .awaitingQueryResult(let blocks):
            // messages arrive in order: profileInfo, progress, data message with 0 rows, data, end of stream
            if case .profileInfo = response {
                return
            }
            if case .progress = response {
                return
            }
            guard case .data(let data) = response else {
                context.fireErrorCaught(ClickHouseError.queryDidNotReturnAnyDataPleaseUseCommand)
                return
            }
            // An empty column count symboles all data is received, next will be an end of stream message
            if data.columnCount == 0 {
                let result = ClickHouseQueryResult(messages: blocks)
                state = .awaitingQueryResultEndOfStream(result: result)
            } else {
                state = .awaitingQueryResult(blocks: blocks + [data])
            }
        case .awaitingQueryResultEndOfStream(let result):
            if case .progress = response {
                return
            }
            guard case .endOfStream = response else {
                context.fireErrorCaught(ClickHouseError.commandReturnedDataPleaseUseQuery)
                return
            }
            state = .ready
            context.fireChannelRead(wrapInboundOut(ClickHouseResult.result(result)))
        case .awaitingToSendData(data: let data):
            guard case ClickHouseMessageDecoder.Result.data(let responseData) = response else {
                fatalError("Inbound Message of 'ClickHouseChannelHandler' had unexpected format")
            }
            precondition(responseData.columns.count == data.count, "Number of columns wrong")
            let dataWithType = zip(data, responseData.columns).map { data, ch -> DataColumnWithType in
                precondition(data.name == ch.column.name, "Column names wrong")
                return DataColumnWithType(column: data, type: ch.type)
            }
            context.writeAndFlush(wrapOutboundOut(.data(data: dataWithType, revision: revision)), promise: nil)
            state = .awaitingQueryConfirmation
        case .awaitingPong:
            guard case .pong = response else {
                context.fireErrorCaught(ClickHouseError.expectedPong)
                return
            }
            state = .ready
            context.fireChannelRead(wrapInboundOut(ClickHouseResult.pong))
        case .awaitingQueryConfirmation:
            if case .profileInfo = response {
                return
            }
            if case .progress = response {
                // For DROP table commands, clickhouse server 20.x sends also a progress message
                return
            }
            if case .data = response {
                // if command is used we are not interested in the data...
                return
            }
            guard case .endOfStream = response else {
                context.fireErrorCaught(ClickHouseError.expectedEndOfStream)
                return
            }
            state = .ready
            context.fireChannelRead(wrapInboundOut(ClickHouseResult.queryExecuted))
        }
    }

    /// Client sends a command TO clickhouse server
    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let request = self.unwrapOutboundIn(data)

        switch state {
        case .notConnected:
            guard case .clientConnect(let database, let user, let password) = request else {
                context.fireErrorCaught(ClickHouseError.expectedConnectCommand)
                return
            }
            logger.debug("Connecting to \(database) user \(user)")
            state = .connecting
            context.writeAndFlush(wrapOutboundOut(.clientConnect(database: database, user: user, password: password)), promise: promise)
        case .ready:
            switch request {
            case .clientConnect:
                context.fireErrorCaught(ClickHouseError.alreadyConnected)
            case .query(let sql):
                logger.debug("Sending query \(sql)")
                context.writeAndFlush(wrapOutboundOut(.query(sql: sql, revision: revision)), promise: promise)
                state = .awaitingQueryResult(blocks: [DataMessage]())
            case .command(let sql):
                logger.debug("Sending command \(sql)")
                context.writeAndFlush(wrapOutboundOut(.query(sql: sql, revision: revision)), promise: promise)
                state = .awaitingQueryConfirmation
            case .insert(let table, let data):
                logger.debug("Inserting \(data.count) columns into table \(table)")
                let columns = data.map { $0.name }.joined(separator: ",")
                let sql = "INSERT INTO \(table) (\(columns)) VALUES"
                context.writeAndFlush(wrapOutboundOut(.query(sql: sql, revision: revision)), promise: promise)
                state = .awaitingToSendData(data: data)
            case .ping:
                logger.debug("Sending ping")
                state = .awaitingPong
                context.writeAndFlush(wrapOutboundOut(.ping), promise: promise)
            }
        default:
            context.fireErrorCaught(ClickHouseError.connectionIsNotReadyToSendNewCommands)
        }
    }

    func close(context: ChannelHandlerContext, mode: CloseMode, promise: EventLoopPromise<Void>?) {
        self.state = .closed
        context.close(mode: mode, promise: promise)
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if (event as? IdleStateHandler.IdleStateEvent) == .read {
            if case .ready = state {
                // If ready, we keep the connection open
            } else {
                self.state = .closed
                self.errorCaught(context: context, error: ClickHouseError.readTimeout)
            }
        } else {
            context.fireUserInboundEventTriggered(event)
        }
    }
}
