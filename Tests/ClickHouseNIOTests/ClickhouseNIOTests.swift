import Logging
import NIO
import XCTest

@testable import ClickHouseNIO

class TestConnection {
    let logger: Logger
    let connection: ClickHouseConnection
    let eventLoopGroup: EventLoopGroup

    init() {
        var logger = ClickHouseLogging.base
        logger.logLevel = .trace
        self.logger = logger

        eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let ip = ProcessInfo.processInfo.environment["CLICKHOUSE_SERVER"] ?? "localhost"
        let user = ProcessInfo.processInfo.environment["CLICKHOUSE_USER"] ?? "default"
        let password = ProcessInfo.processInfo.environment["CLICKHOUSE_PASSWORD"] ?? ""
        logger.info("Connecting to ClickHouse server at \(ip)")
        // openssl req -subj "/CN=my.host.name" -days 365 -nodes -new -x509 -keyout /etc/clickhouse-server/server.key -out /etc/clickhouse-server/server.crt
        // openssl dhparam -out /etc/clickhouse-server/dhparam.pem 1024 // NOTE use 4096 in prod
        // chown -R clickhouse:clickhouse /etc/clickhouse-server/
        // Port 9440 = secure tcp, 9000 regular tcp
        // let tls = TLSConfiguration.forClient(certificateVerification: .none)
        // swiftlint:disable:next force_try
        let config = try! ClickHouseConfiguration(
            hostname: ip,
            port: 9000,
            user: user,
            password: password,
            connectTimeout: .seconds(10),
            readTimeout: .seconds(3),
            queryTimeout: .seconds(5),
            tlsConfiguration: nil
        )
        // swiftlint:disable:next force_try
        connection = try! ClickHouseConnection.connect(configuration: config, on: eventLoopGroup.next()).wait()
    }

    deinit {
        // swiftlint:disable:next force_try
        try! eventLoopGroup.syncShutdownGracefully()
    }
}

final class ClickHouseNIOTests: XCTestCase {
    private var conn = TestConnection()

    func testShowDatabases() {
        XCTAssertNoThrow(try conn.connection.query(sql: "SHOW DATABASES;").map {res in
            print(res)
        }.wait())
    }

    func testPing() throws {
        try conn.connection.ping().wait()
    }

    func testSyntaxError() {
        // Test correct throw on syntax error
        // If invalid SQL is send, and exception is thrown, but the connection is supposed to stay active
        XCTAssertThrowsError(try conn.connection.command(sql: "something wrong").wait(), "awdawf") { e in
            guard case let error as ExceptionMessage = e else {
                XCTFail("Error was not an 'ExceptionMessage'")
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
    func testFixedString() throws {
        try conn.connection.command(sql: "DROP TABLE IF EXISTS test").wait()

        let fixedLength = 7

        let sql = """
            CREATE TABLE test
            (
            id String,
            string FixedString(\(fixedLength))
            )
            ENGINE = MergeTree() PRIMARY KEY id ORDER BY id
            """
        try conn.connection.command(sql: sql).wait()

        let data = [
            ClickHouseColumn("id", ["1", "🎅☃🧪", "234"]),
            ClickHouseColumn("string", ["🎅☃🧪", "a", "awfawfawf"])
        ]

        try conn.connection.insert(into: "test", data: data).wait()

        try conn.connection.query(sql: "SELECT * FROM test").map { res in
            print(res)
            guard let str = res.columns.first(where: { $0.name == "string" })!.values as? [String] else {
                fatalError("Column `string`, was not a String array")
            }
            XCTAssertEqual(str, ["🎅☃", "awfawfa", "a"])
            guard let id = res.columns.first(where: { $0.name == "id" })!.values as? [String] else {
                fatalError("Column `id`, was not a String array")
            }
            XCTAssertEqual(id, ["1", "234", "🎅☃🧪"])
        }.wait()
    }

    func testCreateTable() throws {
        try conn.connection.command(sql: "DROP TABLE IF EXISTS test").wait()

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
        try conn.connection.command(sql: sql).wait()
        let count = 110

        let data = [
            ClickHouseColumn("stationid", (0 ..< count).map { Int32($0) }),
            ClickHouseColumn("timestamp", (0 ..< count).map { Int64($0) }),
            ClickHouseColumn("value", (0 ..< count).map { Float($0) }),
            ClickHouseColumn("varstring", (0 ..< count).map { "\($0)" }),
            ClickHouseColumn("fixstring", (0 ..< count).map { "\($0)" })
        ]

        try conn.connection.insert(into: "test", data: data).wait()

        try conn.connection.query(sql: "SELECT * FROM test").map { res in
            // print(res)
            guard let str = res.columns.first(where: { $0.name == "fixstring" })!.values as? [String] else {
                fatalError("Column 'fixstring' did not exist or wasn't an String-Array")
            }
            XCTAssertEqual((0 ..< count).map { String("\($0)".prefix(2)) }, str)
        }.wait()
    }

    func testTimeout() {
        XCTAssertThrowsError(try conn.connection.command(sql: "SELECT sleep(3)", timeout: .milliseconds(1500)).wait()) { error in
            guard case ClickHouseError.queryTimeout = error else {
                XCTFail("Error wasn't a queryTimeout")
                return
            }
        }
    }

    func testUUID() throws {
        try conn.connection.command(sql: "DROP TABLE IF EXISTS test").wait()
        let sql = """
            CREATE TABLE test
            (
            id Int32,
            uuid UUID
            )
            ENGINE = MergeTree() PRIMARY KEY id ORDER BY id
            """
        try conn.connection.command(sql: sql).wait()

        let uuidStrings: [String] = ["ba4a9cd7-c69c-9fe8-5335-7631f448b597", "ad4f8401-88ff-ca3d-0443-e0163288f691", "5544beae-2370-c5e8-b8b6-c6c46156d28d"]
        let uuids = uuidStrings.map { UUID(uuidString: $0)! }
        let ids: [Int32] = [1, 2, 3]
        print(uuids)
        let data = [
            ClickHouseColumn("id", ids),
            ClickHouseColumn("uuid", uuids)
        ]

        try conn.connection.insert(into: "test", data: data).wait()

        try conn.connection.query(sql: "SELECT id, uuid, toString(uuid) as uuidString FROM test").map { res in
            print(res)
            guard let datatype = res.columns.first(where: { $0.name == "uuidString" })!.values as? [String] else {
                fatalError("Column `uuidString`, was not a String array")
            }
            XCTAssertEqual(datatype, uuidStrings )
            guard let id = res.columns.first(where: { $0.name == "id" })!.values as? [Int32] else {
                fatalError("Column `id`, was not an Int32 array")
            }
            XCTAssertEqual(id, [1, 2, 3])
        }.wait()
    }

    func testCommandForInsertsFromSelectWorks() throws {
        try conn.connection.command(sql: "DROP TABLE IF EXISTS test").wait()

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
        try conn.connection.command(sql: sql).wait()
        let count = 110

        let data = [
            ClickHouseColumn("stationid", (0 ..< count).map { Int32($0) }),
            ClickHouseColumn("timestamp", (0 ..< count).map { Int64($0) }),
            ClickHouseColumn("value", (0 ..< count).map { Float($0) }),
            ClickHouseColumn("varstring", (0 ..< count).map { "\($0)" }),
            ClickHouseColumn("fixstring", (0 ..< count).map { "\($0)" }),
            ClickHouseColumn("nullable", (0 ..< count).map { $0 < 0 ? nil : UInt32($0) })
        ]

        try conn.connection.insert(into: "test", data: data).wait()

        // insert again, but this time via a select from the database
        try conn.connection.command(sql: "Insert into test Select * from test").wait()
    }

    func testNullable() throws {
        try conn.connection.command(sql: "DROP TABLE IF EXISTS test").wait()

        let sql = """
            CREATE TABLE test
            (
            id Int32,
            nullable Nullable(UInt32),
            str Nullable(String)
            )
            ENGINE = MergeTree() PRIMARY KEY id ORDER BY id
            """
        try conn.connection.command(sql: sql).wait()
        let data = [
            ClickHouseColumn("id", [Int32(1), 2, 3, 3, 4, 5, 6, 7, 8, 9]),
            ClickHouseColumn("nullable", [nil, nil, UInt32(1), 3, 4, 5, 6, 7, 8, 8]),
            ClickHouseColumn("str", [nil, nil, "1", "3", "4", "5", "6", "7", "8", "8"])
        ]

        try conn.connection.insert(into: "test", data: data).wait()

        print("send complete")

        try conn.connection.query(sql: "SELECT nullable.null FROM test").map { res in
            // print(res)
            guard let null = res.columns[0].values as? [UInt8?] else {
                fatalError("Column `nullable`, was not a Nullable UInt32 array")
            }
            XCTAssertEqual(null, [1, 1, 0, 0, 0, 0, 0, 0, 0, 0])
        }.wait()

        try conn.connection.query(sql: "SELECT nullable, str FROM test").map { res in
            // print(res)
            XCTAssertEqual(res.columns.count, 2)
            XCTAssertEqual(res.columns[0].name, "nullable")
            guard let id = res.columns[0].values as? [UInt32?] else {
                fatalError("Column `nullable`, was not a Nullable UInt32 array")
            }
            XCTAssertEqual(id, [nil, nil, UInt32(1), 3, 4, 5, 6, 7, 8, 8])

            XCTAssertEqual(res.columns[1].name, "str")
            guard let str = res.columns[1].values as? [String?] else {
                fatalError("Column `nullable`, was not a Nullable UInt32 array")
            }
            XCTAssertEqual(str, [nil, nil, "1", "3", "4", "5", "6", "7", "8", "8"])
        }.wait()
    }

    func testArray() throws {
        try conn.connection.command(sql: "DROP TABLE IF EXISTS test").wait()

        let sql = """
            CREATE TABLE test
            (
            id Int32,
            arr Array(Int32)
            )
            ENGINE = MergeTree() PRIMARY KEY id ORDER BY id
            """
        try conn.connection.command(sql: sql).wait()
        let intArr = [[Int32(1)], [43, 65], [], [1234, -345, 1]]
        let idArr = [Int32(1), 2, 3, 3]
        let data = [
            ClickHouseColumn("id", idArr),
            ClickHouseColumn("arr", intArr)
        ]
        try conn.connection.insert(into: "test", data: data).wait()

        print("send complete")
        try conn.connection.query(sql: "SELECT * FROM test").map { res in
            guard let arr = res.columns[1].values as? [[Int32]] else {
                fatalError("Column `arr`, was not a Int32 array-array")
            }
            XCTAssertEqual(arr, [[1], [43, 65], [], [1234, -345, 1]])
        }.wait()
    }

    func testMap() throws {
        try conn.connection.command(sql: "DROP TABLE IF EXISTS test").wait()

        let sql = """
            CREATE TABLE test
            (
            id Int32,
            m Map(String, String)
            )
            ENGINE = MergeTree() PRIMARY KEY id ORDER BY id
            """
        try conn.connection.command(sql: sql).wait()
        let intArr = [
            [
                "A": "B",
                "C": "D",
                "E": "F",
                "G": "H"
            ],
            [
                "1": "2",
                "3": "4",
                "5": "6"
            ],
            [:],
            [
                "A1": "B2",
                "C3": "D4",
                "E5": "F6",
                "G7": "H8",
                "I9": "J0"
            ]
        ]
        let idArr = [Int32(1), 2, 3, 3]
        let data = [
            ClickHouseColumn("id", idArr),
            ClickHouseColumn("m", intArr)
        ]
        try conn.connection.insert(into: "test", data: data).wait()

        print("send complete")
        try conn.connection.query(sql: "SELECT * FROM test").map { res in
            guard let arr = res.columns[1].values as? [[String: String]] else {
                fatalError("Column `arr`, was not a String map")
            }
            XCTAssertEqual(
                arr,
                [
                    [
                        "A": "B",
                        "C": "D",
                        "E": "F",
                        "G": "H"
                    ],
                    [
                        "1": "2",
                        "3": "4",
                        "5": "6"
                    ],
                    [:],
                    [
                        "A1": "B2",
                        "C3": "D4",
                        "E5": "F6",
                        "G7": "H8",
                        "I9": "J0"
                    ]
                ]
            )
        }.wait()
    }

    func testDate() throws {
        try conn.connection.command(sql: "DROP TABLE IF EXISTS test").wait()

        let sql = """
            CREATE TABLE test
            (
            id Int32,
            date Date,
            dateTime DateTime,
            dateTimeT DateTime('GMT'),
            dateTime64 DateTime64(3, 'GMT')
            )
            ENGINE = MergeTree() PRIMARY KEY id ORDER BY id
            """
            // date32 Date32,
        try conn.connection.command(sql: sql).wait()
        let data = [
            ClickHouseColumn("id", [Int32(1), 2, 3, 4, 5]),
            ClickHouseColumn("date", [
                ClickHouseDate(Date(timeIntervalSince1970: 0)),
                ClickHouseDate(Date(timeIntervalSince1970: 1287842244)),
                ClickHouseDate(Date(timeIntervalSince1970: 5662224000)),
                ClickHouseDate(Date(timeIntervalSince1970: 5662267200)),
                ClickHouseDate(Date(timeIntervalSince1970: -300))
            ]),
            // NOTE: The server we connect to doesn't support Date32 I think?
            // ClickHouseColumn("date32", [
            //     ClickHouseDate32(Date(timeIntervalSince1970: -2_208_988_800.0)),
            //     ClickHouseDate32(Date(timeIntervalSince1970: 1287842244)),
            //     ClickHouseDate32(Date(timeIntervalSince1970: 10_413_791_999.9)),
            //     ClickHouseDate32(Date(timeIntervalSince1970: 5662267200)),
            //     ClickHouseDate32(Date(timeIntervalSince1970: -300))
            // ]),
            ClickHouseColumn("dateTime", [
                ClickHouseDateTime(Date(timeIntervalSince1970: 0)),
                ClickHouseDateTime(Date(timeIntervalSince1970: 1287842244)),
                ClickHouseDateTime(Date(timeIntervalSince1970: 4294967295)),
                ClickHouseDateTime(Date(timeIntervalSince1970: 0)),
                ClickHouseDateTime(Date(timeIntervalSince1970: 0))
            ]),
            ClickHouseColumn("dateTimeT", [
                ClickHouseDateTime(Date(timeIntervalSince1970: 0)),
                ClickHouseDateTime(Date(timeIntervalSince1970: 1287842244)),
                ClickHouseDateTime(Date(timeIntervalSince1970: 4294967295)),
                ClickHouseDateTime(Date(timeIntervalSince1970: 0)),
                ClickHouseDateTime(Date(timeIntervalSince1970: 0))
            ]),
            ClickHouseColumn("dateTime64", [
                ClickHouseDateTime64(Date(timeIntervalSince1970: -2_208_988_800.0)),
                ClickHouseDateTime64(Date(timeIntervalSince1970: 1287842244)),
                ClickHouseDateTime64(Date(timeIntervalSince1970: 10_413_791_999.9)),
                ClickHouseDateTime64(Date(timeIntervalSince1970: 10_413_891_999.9)),
                ClickHouseDateTime64(Date(timeIntervalSince1970: -300))
            ])
        ]

        try conn.connection.insert(into: "test", data: data).wait()

        print("send complete")

        try conn.connection.query(sql: "SELECT date FROM test").map { res in
            // print(res)
            guard let date = res.columns[0].values as? [ClickHouseDate] else {
                fatalError("Column `nullable`, was not a Nullable UInt32 array")
            }
            XCTAssertEqual(
                date.map({ $0.date }),
                [
                    Date(timeIntervalSince1970: 0),
                    Date(timeIntervalSince1970: 1287792000),
                    Date(timeIntervalSince1970: 5662224000),
                    Date(timeIntervalSince1970: 5662224000),
                    Date(timeIntervalSince1970: 0)
                ]
            )
        }.wait()
        // try conn.connection.query(sql: "SELECT date32 FROM test").map { res in
        //     //print(res)
        //     guard let date32 = res.columns[0].values as? [ClickHouseDate32] else {
        //         fatalError("Column `nullable`, was not a Nullable UInt32 array")
        //     }
        //     XCTAssertEqual(date32.map({ $0.date }),
        //         [
        //             Date(timeIntervalSince1970: -2_208_988_800.0),
        //             Date(timeIntervalSince1970: 1287842244),
        //             Date(timeIntervalSince1970: 10_413_791_999.9),
        //             Date(timeIntervalSince1970: 5662267200),
        //             Date(timeIntervalSince1970: -300)
        //         ]
        //     )
        // }.wait()
        try conn.connection.query(sql: "SELECT dateTime FROM test").map { res in
            // print(res)
            guard let dateTime = res.columns[0].values as? [ClickHouseDateTime] else {
                fatalError("Column `nullable`, was not a Nullable UInt32 array")
            }
            XCTAssertEqual(
                dateTime.map({ $0.date }),
                [
                    Date(timeIntervalSince1970: 0),
                    Date(timeIntervalSince1970: 1287842244),
                    Date(timeIntervalSince1970: 4294967295),
                    Date(timeIntervalSince1970: 0),
                    Date(timeIntervalSince1970: 0)
                ]
            )
        }.wait()
        try conn.connection.query(sql: "SELECT dateTimeT FROM test").map { res in
            // print(res)
            guard let dateTime = res.columns[0].values as? [ClickHouseDateTime] else {
                fatalError("Column `nullable`, was not a Nullable UInt32 array")
            }
            XCTAssertEqual(
                dateTime.map({ $0.date }),
                [
                    Date(timeIntervalSince1970: 0),
                    Date(timeIntervalSince1970: 1287842244),
                    Date(timeIntervalSince1970: 4294967295),
                    Date(timeIntervalSince1970: 0),
                    Date(timeIntervalSince1970: 0)
                ]
            )
        }.wait()
        try conn.connection.query(sql: "SELECT dateTime64 FROM test").map { res in
            // print(res)
            guard let dateTime64 = res.columns[0].values as? [ClickHouseDateTime64] else {
                fatalError("Column `nullable`, was not a Nullable UInt32 array")
            }
            XCTAssertEqual(
                dateTime64.map({ $0.date }),
                [
                    Date(timeIntervalSince1970: -2_208_988_800.0),
                    Date(timeIntervalSince1970: 1287842244),
                    Date(timeIntervalSince1970: 10_413_791_999.9),
                    Date(timeIntervalSince1970: 10_413_791_999.9),
                    Date(timeIntervalSince1970: -300)
                ]
            )
        }.wait()
    }

    func testEnum() throws {
        try conn.connection.command(sql: "DROP TABLE IF EXISTS test").wait()

        let sql = """
            CREATE TABLE test
            (
            id Int32,
            e8 Enum8('hi' = -1, 'bye' = 5, 'close' = 10)
            )
            ENGINE = MergeTree() PRIMARY KEY id ORDER BY id
            """
        try conn.connection.command(sql: sql).wait()
        let data = [
            ClickHouseColumn("id", [Int32(1), 2, 3]),
            ClickHouseColumn("e8", [ClickHouseEnum8(word: "hi"), ClickHouseEnum8(word: "close"), ClickHouseEnum8(word: "bye")])
        ]

        try conn.connection.insert(into: "test", data: data).wait()

        print("send complete")

        try conn.connection.query(sql: "SELECT e8 FROM test").map { res in
            // print(res)
            guard let e8 = res.columns[0].values as? [ClickHouseEnum8] else {
                fatalError("Column `nullable`, was not a Nullable UInt32 array")
            }
            XCTAssertEqual(e8.map { $0.word }, ["hi", "close", "bye"])
        }.wait()
    }

    func testArrayMetadata() throws {
        try conn.connection.command(sql: "DROP TABLE IF EXISTS test").wait()

        let sql = """
            CREATE TABLE test
            (
            id Int32,
            arM Array( Enum8('hi' = -1, 'bye' = 5, 'close' = 10) )
            )
            ENGINE = MergeTree() PRIMARY KEY id ORDER BY id
            """
        try conn.connection.command(sql: sql).wait()
        let data = [
            ClickHouseColumn("id", [Int32(1), 2, 3, 4]),
            ClickHouseColumn("arM", [
                [ClickHouseEnum8(word: "hi"), ClickHouseEnum8(word: "close"), ClickHouseEnum8(word: "bye")],
                [],
                [ClickHouseEnum8(word: "hi"), ClickHouseEnum8(word: "close"), ClickHouseEnum8(word: "hi"), ClickHouseEnum8(word: "close")],
                [ClickHouseEnum8(word: "hi"), ClickHouseEnum8(word: "bye"), ClickHouseEnum8(word: "bye"), ClickHouseEnum8(word: "close"), ClickHouseEnum8(word: "bye")]
            ])
        ]

        try conn.connection.insert(into: "test", data: data).wait()

        print("send complete")

        try conn.connection.query(sql: "SELECT arM FROM test").map { res in
            guard let enumArr = res.columns[0].values as? [[ClickHouseEnum8]] else {
                fatalError("Column `nullable`, was not a Nullable UInt32 array")
            }
            XCTAssertEqual(enumArr.map({ $0.map({ $0.word }) }), [
                ["hi", "close", "bye"],
                [],
                ["hi", "close", "hi", "close"],
                ["hi", "bye", "bye", "close", "bye"]
            ])
        }.wait()
    }

    // NOTE: this works locally, but on the server we connect to, bools get intepreted as Int8s
    // func testBool() throws {
    //     try conn.connection.command(sql: "DROP TABLE IF EXISTS test").wait()

    //     let sql = """
    //         CREATE TABLE test
    //         (
    //         id Int32,
    //         b Bool
    //         )
    //         ENGINE = MergeTree() PRIMARY KEY id ORDER BY id
    //         """
    //     try conn.connection.command(sql: sql).wait()
    //     let bArr = [true, false, false, true,]
    //     let data = [
    //         ClickHouseColumn("id", [Int32(1),2,3,3,]),
    //         ClickHouseColumn("b", bArr)
    //     ]

    //     try conn.connection.insert(into: "test", data: data).wait()

    //     try conn.connection.query(sql: "SELECT b FROM test").map { res in
    //         guard let bools = res.columns[0].values as? [Bool] else {
    //             fatalError("is not bool")
    //         }
    //         XCTAssertEqual(bools, [true, false, false, true])
    //     }.wait()
    // }
}
