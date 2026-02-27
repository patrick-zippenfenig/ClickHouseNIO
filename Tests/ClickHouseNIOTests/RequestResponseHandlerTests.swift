import NIO
import XCTest

@testable import ClickHouseNIO

final class RequestResponseHandlerTests: XCTestCase {

    /// Reproduces the memory leak: when a channel becomes inactive while promises
    /// are still outstanding in RequestResponseHandler, those promises must be
    /// failed. Without the `channelInactive` implementation, they would hang
    /// forever and leak.
    func testOutstandingPromisesAreFailedOnChannelInactive() throws {
        let channel = EmbeddedChannel()
        try channel.pipeline.addHandler(
            RequestResponseHandler<String, String>()
        ).wait()

        // Enqueue a request with an outstanding promise
        let promise: EventLoopPromise<String> = channel.eventLoop.makePromise()
        let requestPair: (String, EventLoopPromise<String>) = ("request", promise)
        try channel.writeOutbound(requestPair)

        // Track whether the promise was completed and how
        var promiseResult: Result<String, Error>?
        promise.futureResult.whenComplete { result in
            promiseResult = result
        }

        XCTAssertNil(promiseResult, "Promise should not be completed yet")

        // Simulate the channel closing (e.g. remote peer disconnect)
        channel.pipeline.fireChannelInactive()

        // The promise must have been failed immediately upon channelInactive
        XCTAssertNotNil(
            promiseResult,
            "Promise was not failed on channelInactive — callers will hang until the channel is deallocated"
        )

        if case .failure = promiseResult {
            // expected
        } else {
            XCTFail("Promise should have failed with an error, got: \(String(describing: promiseResult))")
        }
    }

    /// Verifies that multiple outstanding promises are all failed on channel close.
    func testMultipleOutstandingPromisesAreFailedOnChannelInactive() throws {
        let channel = EmbeddedChannel()
        try channel.pipeline.addHandler(
            RequestResponseHandler<String, String>()
        ).wait()

        var results = [Result<String, Error>?](repeating: nil, count: 5)

        for i in 0..<5 {
            let p: EventLoopPromise<String> = channel.eventLoop.makePromise()
            let pair: (String, EventLoopPromise<String>) = ("request-\(i)", p)
            try channel.writeOutbound(pair)
            p.futureResult.whenComplete { result in
                results[i] = result
            }
        }

        channel.pipeline.fireChannelInactive()

        for (i, result) in results.enumerated() {
            XCTAssertNotNil(result, "Promise \(i) was not failed on channelInactive — it would leak")
        }
    }

    /// Confirms that a promise fulfilled normally (before channel close) still works.
    func testPromiseFulfilledBeforeChannelInactive() throws {
        let channel = EmbeddedChannel()
        try channel.pipeline.addHandler(
            RequestResponseHandler<String, String>()
        ).wait()

        let promise: EventLoopPromise<String> = channel.eventLoop.makePromise()
        let pair: (String, EventLoopPromise<String>) = ("request", promise)
        try channel.writeOutbound(pair)

        // Simulate a response arriving before the channel goes inactive
        try channel.writeInbound("response")

        let result = try promise.futureResult.wait()
        XCTAssertEqual(result, "response")

        // Now close — should be a no-op since there are no outstanding promises
        channel.pipeline.fireChannelInactive()
    }
}
