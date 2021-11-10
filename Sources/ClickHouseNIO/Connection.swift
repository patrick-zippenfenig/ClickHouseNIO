import NIO
import NIOSSL
import Logging
@_exported import class NIO.EventLoopFuture
@_exported import struct NIOSSL.TLSConfiguration
@_exported import struct NIO.TimeAmount

public struct ClickHouseConfiguration {
    public let serverAddresses: SocketAddress
    public let user: String
    public let password: String
    public let database: String
    public let tlsConfiguration: TLSConfiguration?
    public let connectTimeout: TimeAmount
    public let readTimeout: TimeAmount
    public let queryTimeout: TimeAmount
    
    public init(
        serverAddresses: SocketAddress,
        user: String? = nil,
        password: String? = nil,
        database: String? = nil,
        connectTimeout: TimeAmount = .seconds(10),
        readTimeout: TimeAmount = .seconds(90),
        queryTimeout: TimeAmount = .seconds(600),
        tlsConfiguration: TLSConfiguration? = nil
    ) {
        self.serverAddresses = serverAddresses
        self.user = user ?? "default"
        self.password = password ?? "admin"
        self.database = database ?? "default"
        self.tlsConfiguration = tlsConfiguration
        self.readTimeout = readTimeout
        self.queryTimeout = queryTimeout
        self.connectTimeout = connectTimeout
    }
    
    public init(
        hostname: String = "localhost",
        port: Int = ClickHouseConnection.defaultPort,
        user: String? = nil,
        password: String? = nil,
        database: String? = nil,
        connectTimeout: TimeAmount = .seconds(10),
        readTimeout: TimeAmount = .seconds(90),
        queryTimeout: TimeAmount = .seconds(600),
        tlsConfiguration: TLSConfiguration? = nil
    ) throws {
        try self.init(
            serverAddresses: .makeAddressResolvingHost(hostname, port: port),
            user: user,
            password: password,
            database: database,
            connectTimeout: connectTimeout,
            readTimeout: readTimeout,
            queryTimeout: queryTimeout,
            tlsConfiguration: tlsConfiguration
        )
    }
}

public class ClickHouseConnection {
    static let DBMS_VERSION_MAJOR : UInt64 = 1;
    static let DBMS_VERSION_MINOR : UInt64 = 1;
    static let REVISION : UInt64           = 54126;
    public static var defaultPort = 9000
    
    internal let channel: Channel
    
    internal let queryTimeout: TimeAmount
    
    /// Set to true if `close()` was called
    private var _isClosed = false
    
    /// Also check if the channel is ok. After exceptions are thrown, the channel become inactive
    public var isClosed: Bool {
        return !channel.isActive || _isClosed
    }
    
    public var eventLoop: EventLoop {
        return channel.eventLoop
    }
    
    fileprivate init(channel: Channel, queryTimeout: TimeAmount) {
        self.channel = channel
        self.queryTimeout = queryTimeout
    }

    public static func connect(configuration: ClickHouseConfiguration, on eventLoop: EventLoop, logger: Logger = .clickHouseBaseLogger) -> EventLoopFuture<ClickHouseConnection> {
        let client = ClientBootstrap(group: eventLoop).connectTimeout(configuration.connectTimeout)
        .channelOption(
            ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR),
            value: 1
        ).channelInitializer { channel in
            do {
                let ssl = try configuration.tlsConfiguration.map { tls -> EventLoopFuture<Void> in
                    let sslContext = try NIOSSLContext(configuration: tls)
                    let handler = try NIOSSLClientHandler(context: sslContext, serverHostname: configuration.serverAddresses.host)
                    return channel.pipeline.addHandler(handler)
                }
                return EventLoopFuture<Void>.andAllSucceed([
                    channel.pipeline.addHandler(IdleStateHandler(readTimeout: configuration.readTimeout)),
                    ssl ?? channel.eventLoop.makeSucceededFuture(()),
                    channel.pipeline.addHandler(MessageToByteHandler(ClickHouseMessageEncoder()), name: "ClickHouseMessageEncoder"),
                    channel.pipeline.addHandler(ByteToMessageHandler(ClickHouseMessageDecoder()), name: "ClickHouseByteDecoder"),
                    channel.pipeline.addHandler(ClickHouseChannelHandler(logger: logger), name: "ClickHouseChannelHandler"),
                    channel.pipeline.addHandler(RequestResponseHandler<ClickHouseCommand, ClickHouseResult>(), name: "RequestResponseHandler")
                ], on: channel.eventLoop)
            } catch {
                return channel.eventLoop.makeFailedFuture(error)
            }
        }
        
        return client.connect(to: configuration.serverAddresses).flatMap { channel in
            return channel.send(.clientConnect(database: configuration.database, user: configuration.user, password: configuration.password), timeout: configuration.queryTimeout).map { res in
                guard case ClickHouseResult.serverInfo(_) = res else {
                    fatalError("ClickHouse did not reply with a serverInfo")
                }
                return ClickHouseConnection(channel: channel, queryTimeout: configuration.queryTimeout)
            }
        }
    }
    
    public func ping(timeout: TimeAmount? = nil) -> EventLoopFuture<Void> {
        return channel.send(.ping, timeout: timeout ?? queryTimeout).map { res in
            guard case ClickHouseResult.pong = res else {
                fatalError("ClickHouse did not reply with poing")
            }
            return
        }
    }
    
    public func query(sql: String, timeout: TimeAmount? = nil) -> EventLoopFuture<ClickHouseQueryResult> {
        return channel.send(.query(sql: sql), timeout: timeout ?? queryTimeout).map { res in
            guard case ClickHouseResult.result(let result) = res else {
                fatalError("ClickHouse did not reply with a query result")
            }
            return result
        }
    }
    
    public func command(sql: String, timeout: TimeAmount? = nil) -> EventLoopFuture<Void> {
        return channel.send(.command(sql: sql), timeout: timeout ?? queryTimeout).map { res in
            guard case ClickHouseResult.queryExecuted = res else {
                fatalError("ClickHouse did not confirm query execution")
            }
            return
        }
    }
    
    public func insert(into table: String, data: [ClickHouseColumnRespresentable], timeout: TimeAmount? = nil) -> EventLoopFuture<Void> {
        return channel.send(.insert(table: table, data: data), timeout: timeout ?? queryTimeout).map { res in
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
    func send(_ command: ClickHouseCommand, timeout: TimeAmount) -> EventLoopFuture<ClickHouseResult> {
        let p: EventLoopPromise<ClickHouseResult> = eventLoop.makePromise()
        let deadline = eventLoop.flatScheduleTask(in: timeout, { () -> EventLoopFuture<Void> in
            p.fail(ClickHouseError.queryTimeout)
            return self.close()
        })
        return writeAndFlush((command, p)).flatMap {
            return p.futureResult.flatMapThrowing { res in
                // Turn "expected" ClickHouse errors into exceptions
                deadline.cancel()
                if case ClickHouseResult.error(let errer) = res {
                    throw errer
                }
                return res
            }
        }.flatMapErrorThrowing { error in
            deadline.cancel()
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

extension SocketAddress {
    /// Hostname if known
    var host: String? {
        switch self {
        case .v4(let v):
            return v.host.isEmpty ? nil : v.host
        case .v6(let v):
            return v.host.isEmpty ? nil : v.host
        case .unixDomainSocket(_):
            return nil
        }
    }
}
