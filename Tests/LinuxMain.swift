import XCTest

import ClickHouseNIOTests

var tests = [XCTestCaseEntry]()
tests += ClickHouseNIOTests.allTests()
XCTMain(tests)
