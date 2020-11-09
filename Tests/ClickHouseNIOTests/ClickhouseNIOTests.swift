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
        logger.logLevel = .debug
        self.logger = logger
        
        eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let ip = ProcessInfo.processInfo.environment["CLICKHOUSE_SERVER"] ?? "172.25.101.30"
        let user = ProcessInfo.processInfo.environment["CLICKHOUSE_USER"] ?? "default"
        let password = ProcessInfo.processInfo.environment["CLICKHOUSE_PASSWORD"] ?? "admin"
        logger.info("Connecting to ClickHouse server at \(ip)")
        let socket = try! SocketAddress(ipAddress: ip, port: 9000)
        let config = ClickHouseConfiguration(serverAddresses: socket, user: user, password: password)
        connection = try! ClickHouseConnection.connect(configuration: config, on: eventLoopGroup.next()).wait()
    }
    
    deinit {
        try! eventLoopGroup.syncShutdownGracefully()
    }
}


final class ClickHouseNIOTests: XCTestCase {
    var conn = TestConnection()
    
    func testShowDatabases() {
        try! conn.connection.query(sql: "SHOW DATABASES;").map{res in
            print(res)
        }.wait()
    }
    
    func testPing() {
        try! conn.connection.ping().wait()
    }
    
    func testSyntaxError() {
        // Test correct throw on syntax error
        // If invalid SQL is send, and exception is thrown, BUT the empty clickhouse block throws a second exception because we send unexpected data.
        // The connection is terminated afterwards by clickhouse
        XCTAssertThrowsError(try conn.connection.command(sql: "something wrong").wait(), "awdawf") { e in
            guard case let error as ExceptionMessage = e else {
                XCTFail()
                return
            }
            XCTAssertEqual(error.code, 1040187392)
            XCTAssertEqual(error.name, "DB::Exception")
            XCTAssertTrue(error.displayText.starts(with: "DB::Exception: Syntax error: failed at position 1: something wrong. Expected one of:"))
        }
        XCTAssertFalse(conn.connection.channel.isActive)
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
}
