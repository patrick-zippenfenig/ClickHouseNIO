import XCTest

import ClickhouseNIOTests

var tests = [XCTestCaseEntry]()
tests += ClickhouseNIOTests.allTests()
XCTMain(tests)
