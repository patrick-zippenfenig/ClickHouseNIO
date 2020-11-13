# ClickHouseNIO

![Swift 5](https://img.shields.io/badge/Swift-5-orange.svg) ![SPM](https://img.shields.io/badge/SPM-compatible-green.svg) ![Platforms](https://img.shields.io/badge/Platforms-macOS%20Linux-green.svg) [![codebeat badge](https://codebeat.co/badges/d15d7e95-d3df-4f97-974c-c3a7d9c07a9e)](https://codebeat.co/projects/github-com-patrick-zippenfenig-clickhousenio-main) [![CircleCI](https://circleci.com/gh/patrick-zippenfenig/ClickHouseNIO/tree/main.svg?style=svg)](https://circleci.com/gh/patrick-zippenfenig/ClickHouseNIO/tree/main) 

High performance Swift [ClickHouse](https://clickhouse.tech) client based on [SwiftNIO 2](https://github.com/apple/swift-nio). It is inspired by the [ClickHouse source code](https://github.com/ClickHouse/ClickHouse/tree/master/src/Client) (C++), but written in pure Swift.

Features:
- Asynchronous Swift NIO implementation. Perfect for concurrent APIs.
- Native Swift data types support. Per table column a simple `[Float]`, `[Int]` or `[String]` array can be used.
- Simple `query()`, `command()` and `insert()` operations

This client provides raw query capabilities. Connection pooling or relational abstraction may be implemented on top of this library. For connection pooling and integration into Vapor use, [ClickHouseVapor](https://github.com/patrick-zippenfenig/ClickHouseVapor).


## Installation:

1. Add `ClickHouseNIO` as a dependency to your `Package.swift`

```swift
  dependencies: [
    .package(url: "https://github.com/patrick-zippenfenig/ClickHouseNIO.git", from: "1.0.0")
  ],
  targets: [
    .target(name: "MyApp", dependencies: ["ClickHouseNIO"])
  ]
```

2. Build your project:

```bash
$ swift build
```

## Usage 

1. Connect to a ClickHouseServer. The client requires a `eventLoop` which is usually provided by frameworks which use SwiftNIO. We also use `wait()` for simplicity, but it is discouraged for production code.

```swift
import NIO
import ClickHouseNIO

let config = try ClickHouseConfiguration(
    hostname: "localhost", 
    port: 9000, 
    user: "default", 
    password: "admin", 
    database: "default")
  
let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)  
let connection = try ClickHouseConnection.connect(configuration: config, on: eventLoopGroup.next()).wait()
```

2. Send commands without data returned. In this example to drop a table. Some DROP or CREATE commands actually do return data. In this case use `query()` instead of `command()`.

```swift
try connection.command(sql: "DROP TABLE IF EXISTS test").wait()
```

3. Create a table

```swift
let sql = """
CREATE TABLE test
(
    id String,
    string FixedString(4)
)
ENGINE = MergeTree() PRIMARY KEY id ORDER BY id
"""
try connection.command(sql: sql).wait()
```

4. Insert data. `ClickHouseColumn` represents a column with an array. String, Float, Double, UUID and Integers are supported.

```swift
let data = [
    ClickHouseColumn("id", ["1","🎅☃🧪","234"]),
    ClickHouseColumn("string", ["🎅☃🧪","a","awfawfawf"])
]

try! connection.insert(into: "test", data: data).wait()
````

5. Query data and cast is to the exptected array

```swift
try! conn.connection.query(sql: "SELECT * FROM test").map { res in
    guard let str = res.columns.first(where: {$0.name == "string"})!.values as? [String] else {
        fatalError("Column `string`, was not a String array")
    }
    XCTAssertEqual(str, ["🎅☃", "awfawfa", "a"])

    guard let id = res.columns.first(where: {$0.name == "id"})!.values as? [String] else {
        fatalError("Column `id`, was not a String array")
    }
    XCTAssertEqual(id, ["1", "234", "🎅☃🧪"])
}.wait()
```



## TODO
- Data message decoding is not optimal, because we grow the buffer until the whole message fits. This could result in reduced performance, the first time a very large query is executed.
- Advanced queries, that report the current `progress` of the query. This could be interesting in the context of Websockets.
- `extremes` feature from ClickHouse to report min/max/mean metrics along the actual data result
- swift metrics support


## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)
