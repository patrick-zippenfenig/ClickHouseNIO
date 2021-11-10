import XCTest
@testable import ClickHouseNIO
import NIO
import Logging


class TestConnection {
    let logger: Logger
    let connection: ClickHouseConnection
    let eventLoopGroup: EventLoopGroup
    
    init() {
        var logger = ClickHouseLogging.base
        logger.logLevel = .trace
        self.logger = logger
        
        eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let ip = ProcessInfo.processInfo.environment["CLICKHOUSE_SERVER"] ?? "172.25.101.30"
        let user = ProcessInfo.processInfo.environment["CLICKHOUSE_USER"] ?? "default"
        let password = ProcessInfo.processInfo.environment["CLICKHOUSE_PASSWORD"] ?? "admin"
        logger.info("Connecting to ClickHouse server at \(ip)")
        // openssl req -subj "/CN=my.host.name" -days 365 -nodes -new -x509 -keyout /etc/clickhouse-server/server.key -out /etc/clickhouse-server/server.crt
        // openssl dhparam -out /etc/clickhouse-server/dhparam.pem 1024 // NOTE use 4096 in prod
        // chown -R clickhouse:clickhouse /etc/clickhouse-server/
        // Port 9440 = secure tcp, 9000 regular tcp
        let socket = try! SocketAddress(ipAddress: ip, port: 9440)
        let tls = TLSConfiguration.forClient(certificateVerification: .none)
        let config = ClickHouseConfiguration(
            serverAddresses: socket, user: user, password: password, connectTimeout: .seconds(10), readTimeout: .seconds(3), queryTimeout: .seconds(5), tlsConfiguration: tls)
        connection = try! ClickHouseConnection.connect(configuration: config, on: eventLoopGroup.next()).wait()
    }
    
    deinit {
        try! eventLoopGroup.syncShutdownGracefully()
    }
}


final class ClickHouseNIOTests: XCTestCase {
    var conn = TestConnection()
    
    func testShowDatabases() {
        XCTAssertNoThrow(try conn.connection.query(sql: "SHOW DATABASES;").map{res in
            print(res)
        }.wait())
    }
    
    func testPing() {
        try! conn.connection.ping().wait()
    }
    
    func testSyntaxError() {
        // Test correct throw on syntax error
        // If invalid SQL is send, and exception is thrown, but the connection is supposed to stay active
        XCTAssertThrowsError(try conn.connection.command(sql: "something wrong").wait(), "awdawf") { e in
            guard case let error as ExceptionMessage = e else {
                XCTFail()
                return
            }
            XCTAssertEqual(error.code, 1040187392)
            XCTAssertEqual(error.name, "DB::Exception")
            XCTAssertTrue(error.displayText.starts(with: "DB::Exception: Syntax error: failed at position 1"))
        }
        XCTAssertNoThrow(try conn.connection.ping().wait())
        XCTAssertFalse(conn.connection.isClosed)
        XCTAssertTrue(conn.connection.channel.isActive)
    }
    
    /// Test correct string truncation with multibyte character
    func testFixedString() {
        try! conn.connection.command(sql: "DROP TABLE IF EXISTS test").wait()
        
        let fixedLength = 7
        
        let sql = """
            CREATE TABLE test
            (
            id String,
            string FixedString(\(fixedLength))
            )
            ENGINE = MergeTree() PRIMARY KEY id ORDER BY id
            """
        try! conn.connection.command(sql: sql).wait()
        
        let data = [
            ClickHouseColumn("id", ["1","🎅☃🧪","234"]),
            ClickHouseColumn("string", ["🎅☃🧪","a","awfawfawf"])
        ]
        
        try! conn.connection.insert(into: "test", data: data).wait()
        
        try! conn.connection.query(sql: "SELECT * FROM test").map { res in
            print(res)
            guard let str = res.columns.first(where: {$0.name == "string"})!.values as? [String] else {
                fatalError("Column `string`, was not a String array")
            }
            XCTAssertEqual(str, ["🎅☃", "awfawfa", "a"])
            guard let id = res.columns.first(where: {$0.name == "id"})!.values as? [String] else {
                fatalError("Column `id`, was not a String array")
            }
            XCTAssertEqual(id, ["1", "234", "🎅☃🧪"])
        }.wait()
    }
    
    func testCreateTable() {
        try! conn.connection.command(sql: "DROP TABLE IF EXISTS test").wait()
        
        let sql = """
            CREATE TABLE test
            (
                stationid Int32,
                timestamp Int64,
                value Float32,
                varstring String,
                fixstring FixedString(2)
            )
            ENGINE = MergeTree() PRIMARY KEY stationid ORDER BY stationid
            """
        try! conn.connection.command(sql: sql).wait()
        let count = 110
        
        let data = [
            ClickHouseColumn("stationid", (0..<count).map{Int32($0)}),
            ClickHouseColumn("timestamp", (0..<count).map{Int64($0)}),
            ClickHouseColumn("value", (0..<count).map{Float($0)}),
            ClickHouseColumn("varstring", (0..<count).map{"\($0)"}),
            ClickHouseColumn("fixstring", (0..<count).map{"\($0)"})
        ]
        
        try! conn.connection.insert(into: "test", data: data).wait()
        
        try! conn.connection.query(sql: "SELECT * FROM test").map { res in
            //print(res)
            guard let str = res.columns.first(where: {$0.name == "fixstring"})!.values as? [String] else {
                fatalError()
            }
            XCTAssertEqual((0..<count).map{String("\($0)".prefix(2))}, str)
        }.wait()
    }
    
    func testTimeout() {
        XCTAssertThrowsError(try conn.connection.command(sql: "SELECT sleep(3)", timeout: .milliseconds(1500)).wait()) { error in
            guard case ClickHouseError.queryTimeout = error else {
                XCTFail()
                return
            }
        }
    }
    
    func testUUID() {
        try! conn.connection.command(sql: "DROP TABLE IF EXISTS test").wait()
        let sql = """
            CREATE TABLE test
            (
            id Int32,
            uuid UUID
            )
            ENGINE = MergeTree() PRIMARY KEY id ORDER BY id
            """
        try! conn.connection.command(sql: sql).wait()
        
        let uuidStrings : [String] = ["ba4a9cd7-c69c-9fe8-5335-7631f448b597", "ad4f8401-88ff-ca3d-0443-e0163288f691", "5544beae-2370-c5e8-b8b6-c6c46156d28d"]
        let uuids = uuidStrings.map { UUID(uuidString: $0)!}
        let ids : [Int32] = [1, 2, 3]
        print(uuids)
        let data = [
            ClickHouseColumn("id", ids),
            ClickHouseColumn("uuid", uuids)
        ]
        
        try! conn.connection.insert(into: "test", data: data).wait()
        
        try! conn.connection.query(sql: "SELECT id, uuid, toString(uuid) as uuidString FROM test").map { res in
            print(res)
            guard let datatype = res.columns.first(where: {$0.name == "uuidString"})!.values as? [String] else {
                fatalError("Column `uuidString`, was not a String array")
            }
            XCTAssertEqual(datatype, uuidStrings )
            guard let id = res.columns.first(where: {$0.name == "id"})!.values as? [Int32] else {
                fatalError("Column `id`, was not an Int32 array")
            }
            XCTAssertEqual(id, [1, 2, 3])
        }.wait()
    }
    
    
    func testCommandForInsertsFromSelectWorks() {
        try! conn.connection.command(sql: "DROP TABLE IF EXISTS test").wait()
        
        let sql = """
            CREATE TABLE test
            (
                stationid Int32,
                timestamp Int64,
                value Float32,
                varstring String,
                fixstring FixedString(2),
                nullable Nullable(UInt32)
            )
            ENGINE = MergeTree() PRIMARY KEY stationid ORDER BY stationid
            """
        try! conn.connection.command(sql: sql).wait()
        let count = 110
        
        let data = [
            ClickHouseColumn("stationid", (0..<count).map{Int32($0)}),
            ClickHouseColumn("timestamp", (0..<count).map{Int64($0)}),
            ClickHouseColumn("value", (0..<count).map{Float($0)}),
            ClickHouseColumn("varstring", (0..<count).map{"\($0)"}),
            ClickHouseColumn("fixstring", (0..<count).map{"\($0)"}),
            ClickHouseColumn("nullable", (0..<count).map{ $0 < 0 ? nil : UInt32($0)})
        ]
        
        try! conn.connection.insert(into: "test", data: data).wait()
        
        // insert again, but this time via a select from the database
        try! conn.connection.command(sql: "Insert into test Select * from test").wait()
    }
    
    
    func testNullable() {
        try! conn.connection.command(sql: "DROP TABLE IF EXISTS test").wait()
        
        let sql = """
            CREATE TABLE test
            (
            id Int32,
            nullable Nullable(UInt32)
            )
            ENGINE = MergeTree() PRIMARY KEY id ORDER BY id
            """
        try! conn.connection.command(sql: sql).wait()
        let ids = [Int32(1), 2, 3]
        let data = [
            ClickHouseColumn("id", ids),
            ClickHouseColumn("nullable", [UInt32(5), 1, 0])
        ]
        
        try! conn.connection.insert(into: "test", data: data).wait()
        
        try! conn.connection.query(sql: "SELECT * FROM test").map { res in
            print(res)
            XCTAssertEqual(res.columns.count, 2)
            XCTAssertEqual(res.columns[1].name, "nullable")
            guard let id = res.columns[1].values as? [UInt32?] else {
                fatalError("Column `nullable`, was not a Nullable UInt32 array")
            }
            XCTAssertEqual(id, [UInt32(5), 1, 0])
        }.wait()
    }
}
