import NIO
import Logging
@_exported import class NIO.EventLoopFuture

public struct ClickHouseConfiguration {
    public var serverAddresses: SocketAddress
    public var user: String
    public var password: String
    public var database: String
    
    public init(serverAddresses: SocketAddress, user: String? = nil, password: String? = nil, database: String? = nil) {
        self.serverAddresses = serverAddresses
        self.user = user ?? "default"
        self.password = password ?? "admin"
        self.database = database ?? "default"
    }
    
    public init(
        hostname: String = "localhost",
        port: Int = ClickHouseConnection.defaultPort,
        user: String? = nil,
        password: String? = nil,
        database: String? = nil
    ) throws {
        try self.init(
            serverAddresses: .makeAddressResolvingHost(hostname, port: port),
            user: user,
            password: password,
            database: database
        )
    }
}

public class ClickHouseConnection {
    static let DBMS_VERSION_MAJOR : UInt64 = 1;
    static let DBMS_VERSION_MINOR : UInt64 = 1;
    static let REVISION : UInt64           = 54126;
    public static var defaultPort = 9000
    
    internal let channel: Channel
    
    /// Set to true if `close()` was called
    private var _isClosed = false
    
    /// Also check if the channel is ok. After exceptions are thrown, the channel become inactive
    public var isClosed: Bool {
        return !channel.isActive || _isClosed
    }
    
    public var eventLoop: EventLoop {
        return channel.eventLoop
    }
    
    fileprivate init(channel: Channel) {
        self.channel = channel
    }

    public static func connect(configuration: ClickHouseConfiguration, on eventLoop: EventLoop, logger: Logger = .clickHouseBaseLogger) -> EventLoopFuture<ClickHouseConnection> {
        let client = ClientBootstrap(group: eventLoop).channelOption(
            ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR),
            value: 1
        ).channelInitializer { channel in
            return EventLoopFuture<Void>.andAllSucceed([
                channel.pipeline.addHandler(MessageToByteHandler(ClickHouseMessageEncoder()), name: "ClickHouseMessageEncoder"),
                channel.pipeline.addHandler(ByteToMessageHandler(ClickHouseMessageDecoder()), name: "ClickHouseByteDecoder"),
                channel.pipeline.addHandler(ClickHouseChannelHandler(logger: logger), name: "ClickHouseChannelHandler"),
                channel.pipeline.addHandler(RequestResponseHandler<ClickHouseCommand, ClickHouseResult>(), name: "RequestResponseHandler")
            ], on: channel.eventLoop)
        }
        
        return client.connect(to: configuration.serverAddresses).flatMap { channel in
            return channel.send(.clientConnect(database: configuration.database, user: configuration.user, password: configuration.password)).map { res in
                guard case ClickHouseResult.serverInfo(_) = res else {
                    fatalError("ClickHouse did not reply with a serverInfo")
                }
                return ClickHouseConnection(channel: channel)
            }
        }
    }
    
    public func ping() -> EventLoopFuture<Void> {
        return channel.send(.ping).map { res in
            guard case ClickHouseResult.pong = res else {
                fatalError("ClickHouse did not reply with poing")
            }
            return
        }
    }
    
    public func query(sql: String) -> EventLoopFuture<ClickHouseQueryResult> {
        return channel.send(.query(sql: sql)).map { res in
            guard case ClickHouseResult.result(let result) = res else {
                fatalError("ClickHouse did not reply with a query result")
            }
            return result
        }
    }
    
    public func command(sql: String) -> EventLoopFuture<Void> {
        return channel.send(.command(sql: sql)).map { res in
            guard case ClickHouseResult.queryExecuted = res else {
                fatalError("ClickHouse did not confirm query execution")
            }
            return
        }
    }
    
    public func insert(into table: String, data: [ClickHouseColumn]) -> EventLoopFuture<Void> {
        return channel.send(.insert(table: table, data: data)).map { res in
            guard case ClickHouseResult.queryExecuted = res else {
                fatalError("ClickHouse did not confirm data insert")
            }
            return
        }
    }
    
    /// Closes this connection.
    public func close() -> EventLoopFuture<Void> {
        _isClosed = true
        return channel.close()
    }
}

extension Channel {
    func send(_ command: ClickHouseCommand) -> EventLoopFuture<ClickHouseResult> {
        // Ensure we are in the correct eventloop
        eventLoop.assertInEventLoop()
        
        let p: EventLoopPromise<ClickHouseResult> = eventLoop.makePromise()
        return writeAndFlush((command, p)).flatMap {
            return p.futureResult.flatMapThrowing { res in
                // Turn "expected" ClickHouse errors into exceptions
                if case ClickHouseResult.error(let errer) = res {
                    throw errer
                }
                return res
            }
        }.flatMapErrorThrowing { error in
            p.fail(error)
            throw error
        }
    }
}

enum ClientCodes : UInt64 {
    case Hello       = 0
    case Query       = 1
    case Data        = 2
    case Cancel      = 3
    case Ping        = 4
}
enum ServerCodes : UInt64 {
    case Hello       = 0;    /// Имя, версия, ревизия.
    case Data        = 1;    /// Блок данных со сжатием или без.
    case Exception   = 2;    /// Исключение во время обработки запроса.
    case Progress    = 3;    /// Прогресс выполнения запроса: строк считано, байт считано.
    case Pong        = 4;    /// Ответ на Ping.
    case EndOfStream = 5;    /// Все пакеты были переданы.
    case ProfileInfo = 6;    /// Пакет с профайлинговой информацией.
    case Totals      = 7;    /// Блок данных с тотальными значениями, со сжатием или без.
    case Extremes    = 8;    /// Блок данных с минимумами и максимумами, аналогично.
}
enum Stages : UInt64 {
    case Complete    = 2;
}
public enum CompressionState : UInt64 {
    case Disable     = 0;
    case Enable      = 1;
}
