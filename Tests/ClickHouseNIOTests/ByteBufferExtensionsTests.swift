import NIO
import XCTest

@testable import ClickHouseNIO

/// Regression tests for a capacity-leak in the `ByteBuffer` write helpers.
///
/// The write helpers used to call `reserveCapacity(N + writableBytes)`, where
/// `writableBytes == capacity - writerIndex`. `ByteBuffer.reserveCapacity`
/// takes a *total* capacity, not "additional" bytes, so after
/// `MessageToByteEncoder.clear()` (which resets `writerIndex` to 0 but keeps
/// capacity), every call became `reserveCapacity(N + currentCapacity)` and the
/// buffer doubled forever. Over a long-lived pooled connection doing many
/// small inserts, this manifested as a multi-GB memory leak with no change in
/// payload size.
///
/// The correct form is `reserveCapacity(writerIndex + N)`, which asks for the
/// total capacity needed after writing `N` more bytes starting from the
/// current writer position.
final class ByteBufferExtensionsTests: XCTestCase {
    /// Simulates the `MessageToByteEncoder` reuse pattern: the same buffer is
    /// cleared between encodes and written to again. If the reserve-capacity
    /// formula is wrong, capacity doubles on every iteration even when the
    /// payload size is constant.
    func testWriteIntegerArrayDoesNotDoubleCapacityAcrossClears() {
        var buf = ByteBufferAllocator().buffer(capacity: 256)
        let array: [UInt64] = [1, 2, 3, 4, 5, 6]

        for _ in 0..<20 {
            buf.clear()
            buf.writeIntegerArray(array)
        }

        // 20 iterations of a 48-byte write should not grow the buffer beyond
        // a small constant. The buggy form grew capacity to ~256 MB at 20
        // iterations (each call ~doubled it), so 4 KB is a comfortable ceiling
        // that still catches any regression to exponential growth.
        XCTAssertLessThanOrEqual(
            buf.capacity,
            4096,
            "ByteBuffer capacity grew unexpectedly — writeIntegerArray is leaking capacity across clears"
        )
    }

    func testWriteOptionalIntegerArrayDoesNotDoubleCapacityAcrossClears() {
        var buf = ByteBufferAllocator().buffer(capacity: 256)
        let array: [UInt64?] = [1, nil, 3, nil, 5, 6]

        for _ in 0..<20 {
            buf.clear()
            buf.writeOptionalIntegerArray(array)
        }

        XCTAssertLessThanOrEqual(buf.capacity, 4096)
    }

    func testWriteClickHouseStringsDoesNotDoubleCapacityAcrossClears() {
        var buf = ByteBufferAllocator().buffer(capacity: 256)
        let strings = ["gateway", "exporter", "auth", "billing", "cdn", "chat"]

        for _ in 0..<20 {
            buf.clear()
            buf.writeClickHouseStrings(strings)
        }

        XCTAssertLessThanOrEqual(buf.capacity, 4096)
    }

    func testWriteClickHouseFixedStringsDoesNotDoubleCapacityAcrossClears() {
        var buf = ByteBufferAllocator().buffer(capacity: 256)
        let strings = ["aaaaaaaa", "bbbbbbbb", "cccccccc", "dddddddd"]

        for _ in 0..<20 {
            buf.clear()
            buf.writeClickHouseFixedStrings(strings, length: 8)
        }

        XCTAssertLessThanOrEqual(buf.capacity, 4096)
    }

    func testWriteUuidArrayDoesNotDoubleCapacityAcrossClears() {
        var buf = ByteBufferAllocator().buffer(capacity: 256)
        let uuids = (0..<4).map { _ in UUID() }

        for _ in 0..<20 {
            buf.clear()
            buf.writeUuidArray(uuids)
        }

        XCTAssertLessThanOrEqual(buf.capacity, 4096)
    }

    /// Sanity: the write helpers still produce the same bytes they did before
    /// — the fix is a pure capacity-math change, it must not touch the wire
    /// format.
    func testWriteIntegerArrayPayloadUnchanged() {
        var buf = ByteBufferAllocator().buffer(capacity: 256)
        buf.writeIntegerArray([UInt64(1), UInt64(2), UInt64(3)])
        XCTAssertEqual(buf.readableBytes, 3 * MemoryLayout<UInt64>.size)
        XCTAssertEqual(buf.readInteger(endianness: .little, as: UInt64.self), 1)
        XCTAssertEqual(buf.readInteger(endianness: .little, as: UInt64.self), 2)
        XCTAssertEqual(buf.readInteger(endianness: .little, as: UInt64.self), 3)
    }
}
