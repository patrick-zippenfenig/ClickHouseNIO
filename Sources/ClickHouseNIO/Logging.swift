//
//  Logging.swift
//  ClickHouseNIO
//
//  Created by Patrick Zippenfenig on 27.10.20.
//

import Foundation
import Logging

public enum ClickHouseLogging {
    public static let base = Logger(label: "ClickHouseNIO.Connection")
}

public extension Logger {
    static var clickHouseBaseLogger: Logger {
        return ClickHouseLogging.base
    }
}
