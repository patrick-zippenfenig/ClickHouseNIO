//
//  Logging.swift
//  ClickHouseNIO
//
//  Created by Patrick Zippenfenig on 27.10.20.
//

import Foundation
import Logging

public struct ClickHouseLogging {
    public static let base = Logger(label: "ClickHouseNIO.Connection")
}

extension Logger {
    public static var clickHouseBaseLogger: Logger {
        return ClickHouseLogging.base
    }
}
