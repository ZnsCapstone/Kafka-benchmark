package com.hanyang.cs;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class KafkaBenchmarkTest {
    @Test
    void distributesTargetRateWithoutLosingRemainder() {
        assertArrayEquals(
                new int[] {2, 2, 2, 1, 1},
                KafkaBenchmark.distributeRate(8, 5));
    }

    @Test
    void supportsMoreProducersThanTargetOperations() {
        assertArrayEquals(
                new int[] {1, 1, 1, 0, 0},
                KafkaBenchmark.distributeRate(3, 5));
    }

    @Test
    void latencyCapacityIncludesOneSecondOfHeadroom() {
        assertEquals(61_024, KafkaBenchmark.latencyCapacity(1_000, 60));
        assertEquals(1_024, KafkaBenchmark.latencyCapacity(0, 60));
    }

    @Test
    void rejectsLatencyArraysLargerThanJvmLimit() {
        assertThrows(
                IllegalArgumentException.class,
                () -> KafkaBenchmark.latencyCapacity(Integer.MAX_VALUE, 2));
    }

    @Test
    void countsSeparateAndConsecutiveAckStalls() {
        assertArrayEquals(
                new int[] {2, 5, 3},
                KafkaBenchmark.calculateAckStalls(new long[] {10, 0, 0, 4, 0, 0, 0, 8}));
    }

    @Test
    void failFastRequiresSustainedStallAndPendingOrFailedWork() {
        long second = 1_000_000_000L;

        assertFalse(KafkaBenchmark.shouldFailFast(59 * second, 0, 1000, 0, 60));
        assertFalse(KafkaBenchmark.shouldFailFast(60 * second, 0, 0, 0, 60));
        assertFalse(KafkaBenchmark.shouldFailFast(600 * second, 0, 1000, 1, 0));
        assertTrue(KafkaBenchmark.shouldFailFast(60 * second, 0, 1000, 0, 60));
        assertTrue(KafkaBenchmark.shouldFailFast(60 * second, 0, 0, 1, 60));
    }
}
