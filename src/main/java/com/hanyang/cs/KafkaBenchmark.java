package com.hanyang.cs;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * =============================================================================
 *  KafkaBenchmark — Kafka 파일시스템 벤치마크 (Milestone #1: ext4 vs f2fs)
 * =============================================================================
 *
 *  목적
 *   - 고정된 Target OP/s 부하에서 파일시스템별 처리량과 응답 지연을 측정.
 *   - Kafka 4.2 / KRaft 단일 브로커 / replication=1 환경을 기준으로 설계.
 *   - ext4와 f2fs 베이스라인을 이후 FEMU + ZNS 결과와 비교.
 *
 *  Two-level latency 분석 전략
 *   이 코드가 측정하는 app latency는 producer.send() 진입부터 acks=1 callback까지다.
 *   producer buffer 대기, network, broker append와 ACK 대기가 모두 포함된다. broker의
 *   append는 일반적으로 OS page cache를 사용하며 acks=1은 디스크 영구 기록을
 *   보장하지 않는다.
 *
 *   따라서 보고서에서는 두 layer를 분리해 분석한다.
 *     - App-level latency  : 이 코드가 측정하는 send -> ACK 시간.
 *     - Block-level latency: iostat await 등 실제 block I/O queue 처리 시간.
 *
 *  비동기 callback send
 *   - producer.send(record, callback)으로 producer thread와 Kafka sender thread가
 *     독립적으로 동작한다.
 *   - callback에서 System.nanoTime() - sendStart로 성공 요청의 latency를 측정한다.
 *   - producer별 latency 배열을 사용하며 callback이 값을 기록한 뒤 AtomicInteger
 *     count를 증가시켜 통계 thread에 유효 범위를 공개한다.
 *
 *  실행 구간
 *   1) Warmup     : 요청은 보내지만 measurement 통계에서는 제외.
 *   2) Measurement: send 시각이 이 구간에 속한 요청을 측정 대상으로 지정.
 *   3) Drain      : 새 요청 생성을 멈추고 이미 보낸 요청의 callback을 기다림.
 *
 *   measurement 중 send된 요청은 drain 중 ACK되어도 Eventual ACK와 latency 표본에
 *   포함된다. ACK Window는 measurement 종료 전에 ACK된 요청만 별도로 센다.
 *
 *  부하 제어
 *   - absolute-time hybrid pacing으로 짧은 간격의 scheduler 오차 누적을 줄인다.
 *   - max-in-flight-records로 애플리케이션 전체 outstanding 요청을 제한할 수 있다.
 *   - max-catch-up-records와 max-schedule-lag-ms로 지연 후 catch-up burst를 제한한다.
 *   - 위 제한은 offered load를 바꿀 수 있으므로 결과의 backpressure/catch-up 지표와
 *     함께 해석해야 한다.
 * =============================================================================
 */
public class KafkaBenchmark {

    // CLI로 덮어쓸 수 있는 접속 및 실행 기본값.
    private static String bootstrapServers = "localhost:9092";
    private static String topicName = "bench-topic";

    private static int recordSize = 1024;
    private static int targetOps = 10000;
    private static int numProducers = 16;
    private static boolean useConsumer = false;
    private static boolean dynamicTopicCreation = false;
    private static int warmupSec = 0;
    private static int measureSec = 60;
    private static int drainTimeoutSec = 180;
    private static int dynamicTopicRate = 1;
    private static int maxInFlightRecords = 0;
    private static int maxCatchUpRecords = 0;
    private static long maxScheduleLagMs = 0;
    private static int producerStartTimeoutSec = 90;

    public static void main(String[] args) throws Exception {
        parseArgs(args);
        validateConfig();

        System.out.println("=====================================");
        System.out.println(" Kafka Filesystem Benchmark (Java API) ");
        System.out.println("=====================================");
        System.out.printf(" - Record Size: %d Bytes%n", recordSize);
        System.out.printf(" - Target OP/s (total): %d%n", targetOps);
        System.out.printf(" - Producers: %d%n", numProducers);
        System.out.printf(" - Consumer Active: %b%n", useConsumer);
        System.out.printf(" - Dynamic Topic Creation: %b (rate=%d/sec)%n",
                dynamicTopicCreation, dynamicTopicRate);
        System.out.printf(" - Warmup: %d sec / Measurement: %d sec / Drain timeout: %d sec%n",
                warmupSec, measureSec, drainTimeoutSec);
        System.out.printf(" - Max outstanding records: %s%n",
                maxInFlightRecords == 0 ? "unlimited" : Integer.toString(maxInFlightRecords));
        System.out.printf(" - Catch-up limit: records=%s, lag=%s%n",
                maxCatchUpRecords == 0 ? "unlimited" : Integer.toString(maxCatchUpRecords),
                maxScheduleLagMs == 0 ? "unlimited" : maxScheduleLagMs + "ms");
        System.out.println(" - Send mode: ASYNC (callback-based latency measurement)");

        AtomicBoolean running = new AtomicBoolean(true);
        CountDownLatch readyLatch = new CountDownLatch(numProducers);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numProducers);

        byte[] payload = new byte[recordSize];
        new Random(42).nextBytes(payload);

        int[] producerRates = distributeRate(targetOps, numProducers);
        Semaphore outstandingLimiter = maxInFlightRecords > 0
                ? new Semaphore(maxInFlightRecords) : null;

        ProducerTask[] tasks = new ProducerTask[numProducers];
        ExecutorService executor = Executors.newFixedThreadPool(
                numProducers + (useConsumer ? 1 : 0) + (dynamicTopicCreation ? 1 : 0));

        for (int i = 0; i < numProducers; i++) {
            tasks[i] = new ProducerTask(
                    i, producerRates[i], payload, running,
                    readyLatch, startLatch, doneLatch,
                    latencyCapacity(producerRates[i], measureSec), outstandingLimiter);
            executor.submit(tasks[i]);
        }

        if (useConsumer) {
            executor.submit(new ConsumerTask(running));
        }

        if (dynamicTopicCreation) {
            executor.submit(new TopicCreatorTask(running, dynamicTopicRate));
        }

        boolean allReady = readyLatch.await(producerStartTimeoutSec, TimeUnit.SECONDS);
        if (!allReady || Arrays.stream(tasks).anyMatch(t -> t.startupError != null)) {
            running.set(false);
            startLatch.countDown();
            executor.shutdownNow();
            Throwable cause = Arrays.stream(tasks)
                    .map(t -> t.startupError).filter(e -> e != null).findFirst().orElse(null);
            throw new IllegalStateException(
                    allReady ? "One or more producers failed to initialize"
                            : "Producer initialization timed out after " + producerStartTimeoutSec + "s",
                    cause);
        }
        long startTimeNs = System.nanoTime();
        ProducerTask.globalStartNs = startTimeNs;
        ProducerTask.globalOutstanding.set(0);
        ProducerTask.globalMaxOutstanding.set(0);
        ProducerTask.measureStartNs = startTimeNs + TimeUnit.SECONDS.toNanos(warmupSec);
        ProducerTask.measureEndNs = ProducerTask.measureStartNs
                + TimeUnit.SECONDS.toNanos(measureSec);
        startLatch.countDown();

        System.out.printf("[Run] Producers started. warmup=%ds, measure=%ds, total=%ds%n",
                warmupSec, measureSec, warmupSec + measureSec);

        Thread.sleep(((long) warmupSec + measureSec) * 1000L);
        running.set(false);

        long outstandingAtEnd = 0;
        for (ProducerTask task : tasks) {
            outstandingAtEnd += task.measuredOutstanding.get();
        }

        long drainStartNs = System.nanoTime();

        // send 중단 후 이미 제출한 요청을 flush한다. timeout run은 통계가 불완전할 수 있다.
        boolean cleanFinish = doneLatch.await(drainTimeoutSec, TimeUnit.SECONDS);
        if (!cleanFinish) {
            System.err.printf("[Warn] Some producers did not finish in %ds. Forcing shutdown.%n",
                    drainTimeoutSec);
        }
        long drainElapsedNs = System.nanoTime() - drainStartNs;

        executor.shutdownNow();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        printMetrics(tasks, drainElapsedNs, cleanFinish, outstandingAtEnd);
    }

    /**
     * producer별 측정 스냅샷을 병합해 latency와 처리량 지표를 출력한다.
     *
     * drain timeout이 발생하면 callback이 계속 실행될 수 있으므로 각 배열의 유효
     * 길이를 먼저 고정한다. 출력 라벨은 Python 결과 파서의 인터페이스이므로 변경할
     * 때 Python 정규식과 CSV schema도 함께 수정해야 한다.
     */
    private static void printMetrics(ProducerTask[] tasks,
                                     long drainElapsedNs,
                                     boolean cleanFinish,
                                     long outstandingAtEnd) {
        long totalSamples = 0;
        long totalSentIncludingWarmup = 0;
        long totalSendErrors = 0;
        long sentRequests = 0;
        long ackWindowRequests = 0;
        long eventualAckRequests = 0;
        long failedRequests = 0;
        long droppedSamples = 0;
        long limiterWaitCount = 0;
        long limiterWaitNs = 0;
        long maxObservedOutstanding = 0;
        long catchUpResets = 0;
        long skippedCatchUp = 0;
        long maxScheduleLagNs = 0;
        int[] sampleCounts = new int[tasks.length];
        for (int taskIndex = 0; taskIndex < tasks.length; taskIndex++) {
            ProducerTask t = tasks[taskIndex];
            int n = Math.min(t.recordedCount.get(), t.latenciesNs.length);
            sampleCounts[taskIndex] = n;
            totalSamples += n;
            totalSentIncludingWarmup += t.totalSent.get();
            totalSendErrors += t.sendErrors.get();
            sentRequests += t.measuredSent.get();
            ackWindowRequests += t.measuredAckInWindow.get();
            eventualAckRequests += t.measuredEventualAck.get();
            failedRequests += t.measuredFailures.get();
            droppedSamples += t.droppedLatencySamples.get();
            limiterWaitCount += t.limiterWaitCount.get();
            limiterWaitNs += t.limiterWaitNs.get();
            maxObservedOutstanding = ProducerTask.globalMaxOutstanding.get();
            catchUpResets += t.catchUpResets.get();
            skippedCatchUp += t.skippedCatchUpRecords.get();
            maxScheduleLagNs = Math.max(maxScheduleLagNs, t.maxScheduleLagNs.get());
        }

        long unresolvedAfterDrain = Math.max(0,
                sentRequests - eventualAckRequests - failedRequests);

        if (totalSamples == 0) {
            System.out.println("\n--- [ Latency Results (ms) ] ---");
            System.out.println(" No samples recorded after warmup.");
            System.out.println(" Total Requests : 0");
            System.out.println(" Average        : 0.00 ms");
            System.out.println(" p50 (Median)   : 0.00 ms");
            System.out.println(" p90            : 0.00 ms");
            System.out.println(" p99            : 0.00 ms");
            System.out.println(" p999           : 0.00 ms");
            System.out.println(" Max            : 0.00 ms");
            System.out.println("--------------------------------");
            System.out.println("--- [ Throughput / Errors ] ---");
            System.out.printf(" Target OP/s         : %d%n", targetOps);
            System.out.println(" Achieved OP/s       : 0.00");
            System.out.println(" Achieved/Target (%) : 0.0");
            System.out.printf(" Total Sent (incl. warmup) : %d%n", totalSentIncludingWarmup);
            System.out.printf(" Send Errors               : %d%n", totalSendErrors);
            System.out.printf(" Drain Time                : %.2f sec%n",
                    drainElapsedNs / 1_000_000_000.0);
            System.out.printf(" Drain Completed           : %b%n", cleanFinish);
            printExtendedMetrics(sentRequests, ackWindowRequests, eventualAckRequests,
                    outstandingAtEnd, failedRequests, unresolvedAfterDrain, droppedSamples,
                    limiterWaitCount, limiterWaitNs, maxObservedOutstanding,
                    catchUpResets, skippedCatchUp, maxScheduleLagNs);
            System.out.println("--------------------------------");
            return;
        }

        long[] all = new long[(int) totalSamples];
        int idx = 0;
        for (int taskIndex = 0; taskIndex < tasks.length; taskIndex++) {
            ProducerTask t = tasks[taskIndex];
            int n = sampleCounts[taskIndex];
            System.arraycopy(t.latenciesNs, 0, all, idx, n);
            idx += n;
        }
        if (idx < all.length) {
            all = Arrays.copyOf(all, idx);
            totalSamples = idx;
        }
        Arrays.sort(all);

        double avgMs = arithmeticMean(all) / 1_000_000.0;
        double p50  = percentile(all, 0.50)  / 1_000_000.0;
        double p90  = percentile(all, 0.90)  / 1_000_000.0;
        double p99  = percentile(all, 0.99)  / 1_000_000.0;
        double p999 = percentile(all, 0.999) / 1_000_000.0;
        double max  = all[all.length - 1] / 1_000_000.0;

        // 호환 지표 Achieved OP/s는 Eventual ACK OP/s와 같은 의미다.
        double achievedOpsPerSec = eventualAckRequests / (double) Math.max(1, measureSec);

        System.out.println("\n--- [ Latency Results (ms) ] ---");
        System.out.println(" (App-level: send() call -> broker ACK; reaches OS page cache)");
        System.out.printf(" Total Requests : %d%n", totalSamples);
        System.out.printf(" Average        : %.2f ms%n", avgMs);
        System.out.printf(" p50 (Median)   : %.2f ms%n", p50);
        System.out.printf(" p90            : %.2f ms%n", p90);
        System.out.printf(" p99            : %.2f ms%n", p99);
        System.out.printf(" p999           : %.2f ms%n", p999);
        System.out.printf(" Max            : %.2f ms%n", max);
        System.out.println("--------------------------------");
        System.out.println("--- [ Throughput / Errors ] ---");
        System.out.printf(" Target OP/s         : %d%n", targetOps);
        System.out.printf(" Achieved OP/s       : %.2f%n", achievedOpsPerSec);
        System.out.printf(" Achieved/Target (%%) : %.1f%n",
                (achievedOpsPerSec / Math.max(1, targetOps)) * 100.0);
        System.out.printf(" Total Sent (incl. warmup) : %d%n", totalSentIncludingWarmup);
        System.out.printf(" Send Errors               : %d%n", totalSendErrors);
        System.out.printf(" Drain Time                : %.2f sec%n",
                drainElapsedNs / 1_000_000_000.0);
        System.out.printf(" Drain Completed           : %b%n", cleanFinish);
        printExtendedMetrics(sentRequests, ackWindowRequests, eventualAckRequests,
                outstandingAtEnd, failedRequests, unresolvedAfterDrain, droppedSamples,
                limiterWaitCount, limiterWaitNs, maxObservedOutstanding,
                catchUpResets, skippedCatchUp, maxScheduleLagNs);
        System.out.println("--------------------------------");
        System.out.println("\n--- [ Per-second Throughput & Latency ] ---");

        for (int sec = 0; sec < warmupSec + measureSec; sec++) {
            long send = 0;
            long ack = 0;
            int count = 0;
            for (int taskIndex = 0; taskIndex < tasks.length; taskIndex++) {
                ProducerTask task = tasks[taskIndex];
                send += task.sendBuckets[sec].get();
                ack += task.ackBuckets[sec].get();
                int n = sampleCounts[taskIndex];
                for (int i = 0; i < n; i++) {
                    if (task.ackSecond[i] == sec) count++;
                }
            }

            if (count == 0) {
                System.out.printf("Sec %3d | send=%6d | ack=%6d | no data\n",
                        sec, send, ack);
                continue;
            }

            long[] arr = new long[count];
            int position = 0;
            for (int taskIndex = 0; taskIndex < tasks.length; taskIndex++) {
                ProducerTask task = tasks[taskIndex];
                int n = sampleCounts[taskIndex];
                for (int i = 0; i < n; i++) {
                    if (task.ackSecond[i] == sec) arr[position++] = task.latenciesNs[i];
                }
            }
            Arrays.sort(arr);

            double secp50 = percentile(arr, 0.50) / 1_000_000.0;
            double secp99 = percentile(arr, 0.99) / 1_000_000.0;

            System.out.printf(
                "Sec %3d | send=%6d | ack=%6d | p50=%.2f ms | p99=%.2f ms\n",
                sec, send, ack, secp50, secp99
            );
        }

        System.out.println("------------------------------------------");
    }

    /**
     * offered load, measurement-window completion, drain completion과 backlog를 분리해
     * 출력한다. 모든 OP/s의 분모는 drain 시간이 아닌 고정된 measurement 시간이다.
     */
    private static void printExtendedMetrics(
            long sent, long ackWindow, long eventualAck, long outstandingAtEnd,
            long failed, long unresolved, long dropped, long limiterWaitCount,
            long limiterWaitNs, long maxOutstanding, long catchUpResets,
            long skippedCatchUp, long maxScheduleLagNs) {
        double seconds = Math.max(1, measureSec);
        System.out.printf(" Sent Requests             : %d%n", sent);
        System.out.printf(" Sent OP/s                 : %.2f%n", sent / seconds);
        System.out.printf(" ACK Window Requests       : %d%n", ackWindow);
        System.out.printf(" ACK Window OP/s           : %.2f%n", ackWindow / seconds);
        System.out.printf(" Eventual ACK Requests     : %d%n", eventualAck);
        System.out.printf(" Eventual ACK OP/s         : %.2f%n", eventualAck / seconds);
        System.out.printf(" Outstanding at End        : %d%n", outstandingAtEnd);
        System.out.printf(" Failed Requests           : %d%n", failed);
        System.out.printf(" Unresolved After Drain    : %d%n", unresolved);
        System.out.printf(" Latency Dropped Samples   : %d%n", dropped);
        System.out.printf(" Backpressure Wait Count   : %d%n", limiterWaitCount);
        System.out.printf(" Backpressure Wait Time    : %.2f ms%n", limiterWaitNs / 1_000_000.0);
        System.out.printf(" Max Observed Outstanding  : %d%n", maxOutstanding);
        System.out.printf(" Catch-up Resets           : %d%n", catchUpResets);
        System.out.printf(" Catch-up Records Skipped  : %d%n", skippedCatchUp);
        System.out.printf(" Max Schedule Lag          : %.2f ms%n", maxScheduleLagNs / 1_000_000.0);
    }

    private static double arithmeticMean(long[] sortedOrAny) {
        double sum = 0;
        for (long v : sortedOrAny) sum += v;
        return sum / sortedOrAny.length;
    }

    private static long percentile(long[] sorted, double p) {
        int n = sorted.length;
        int rank = (int) Math.ceil(p * n) - 1;
        if (rank < 0) rank = 0;
        if (rank >= n) rank = n - 1;
        return sorted[rank];
    }

    /** 전체 target을 producer에 분배하며 나머지는 앞 producer부터 한 건씩 배정한다. */
    static int[] distributeRate(int totalOps, int producers) {
        int[] rates = new int[producers];
        int base = totalOps / producers;
        int remainder = totalOps % producers;
        for (int i = 0; i < producers; i++) {
            rates[i] = base + (i < remainder ? 1 : 0);
        }
        return rates;
    }

    /**
     * measurement 예상 요청 수와 1초 이상의 여유를 확보한다.
     * 정확한 percentile을 위해 표본 전체를 보존하므로 지나치게 큰 실행은 거부한다.
     */
    static int latencyCapacity(int producerOps, int seconds) {
        long expected = (long) producerOps * seconds;
        long capacity = Math.max(1024L, expected + Math.max(1024L, producerOps));
        if (capacity > Integer.MAX_VALUE - 8L) {
            throw new IllegalArgumentException("latency sample capacity exceeds JVM array limit");
        }
        return (int) capacity;
    }

    private static AtomicLong[] atomicBuckets(int size) {
        AtomicLong[] buckets = new AtomicLong[size];
        Arrays.setAll(buckets, ignored -> new AtomicLong());
        return buckets;
    }

    private static void updateMax(AtomicLong target, long candidate) {
        long current;
        do {
            current = target.get();
            if (candidate <= current) return;
        } while (!target.compareAndSet(current, candidate));
    }

    private static long saturatedMultiply(long left, long right) {
        if (left == 0 || right == 0) return 0;
        if (left > Long.MAX_VALUE / right) return Long.MAX_VALUE;
        return left * right;
    }

    /** 메모리 또는 측정 의미를 깨뜨릴 수 있는 CLI 조합을 broker 연결 전에 거부한다. */
    private static void validateConfig() {
        if (recordSize <= 0 || targetOps <= 0 || numProducers <= 0) {
            throw new IllegalArgumentException("record-size, target-ops and producers must be > 0");
        }
        if (recordSize > 10 * 1024 * 1024 - 512) {
            throw new IllegalArgumentException("record-size exceeds configured max.request.size");
        }
        if (warmupSec < 0 || measureSec <= 0 || drainTimeoutSec <= 0
                || dynamicTopicRate <= 0 || producerStartTimeoutSec <= 0) {
            throw new IllegalArgumentException(
                    "warmup-sec must be >= 0; durations and dynamic-topic-rate must be > 0");
        }
        if (maxInFlightRecords < 0 || maxCatchUpRecords < 0 || maxScheduleLagMs < 0) {
            throw new IllegalArgumentException("limiter options must be >= 0");
        }
        if (bootstrapServers.isBlank() || topicName.isBlank()) {
            throw new IllegalArgumentException("bootstrap-servers and topic must not be blank");
        }
        if ((long) targetOps * measureSec > Integer.MAX_VALUE - 8L) {
            throw new IllegalArgumentException(
                    "target-ops * measure-sec exceeds exact percentile sample limit");
        }
        Math.addExact(warmupSec, measureSec);
    }

    /**
     * 한 KafkaProducer의 pacing, 비동기 전송과 callback 통계를 담당한다.
     *
     * producer별 latency 배열에는 measurement 중 send된 성공 요청만 기록한다.
     * callback thread가 값을 쓴 뒤 {@code recordedCount}를 증가시키므로 통계 thread는
     * count까지의 값만 읽어야 한다. limiter를 사용할 때 permit은 callback 또는 동기
     * send 실패 경로에서 반드시 반환한다.
     */
    static class ProducerTask implements Runnable {
        // startLatch를 열기 전에 설정되는 공통 monotonic 시간 경계.
        static volatile long globalStartNs = 0L;
        static volatile long measureStartNs = 0L;
        static volatile long measureEndNs = 0L;
        static final AtomicLong globalOutstanding = new AtomicLong(0);
        static final AtomicLong globalMaxOutstanding = new AtomicLong(0);

        private final int id;
        private final int opsPerSec;
        private final byte[] payload;
        private final AtomicBoolean running;
        private final CountDownLatch readyLatch;
        private final CountDownLatch startLatch;
        private final CountDownLatch doneLatch;
        private final Semaphore outstandingLimiter;

        // latency와 ACK 초는 동일 인덱스를 사용한다. capacity 초과는 별도 지표로 센다.
        final long[] latenciesNs;
        final int[] ackSecond;
        final AtomicInteger recordedCount = new AtomicInteger(0);

        // 전체 실행 호환 지표: warmup을 포함하며 기존 Python parser가 사용한다.
        final AtomicLong totalSent = new AtomicLong(0);
        final AtomicLong sendErrors = new AtomicLong(0);

        // measurement 지표: offered load, window 내 완료, drain까지의 완료를 구분한다.
        final AtomicLong measuredSent = new AtomicLong(0);
        final AtomicLong measuredAckInWindow = new AtomicLong(0);
        final AtomicLong measuredEventualAck = new AtomicLong(0);
        final AtomicLong measuredFailures = new AtomicLong(0);
        final AtomicLong measuredOutstanding = new AtomicLong(0);
        final AtomicLong droppedLatencySamples = new AtomicLong(0);

        // limiter와 pacing이 실제 부하 생성에 준 영향을 설명하는 진단 지표다.
        final AtomicLong limiterWaitCount = new AtomicLong(0);
        final AtomicLong limiterWaitNs = new AtomicLong(0);
        final AtomicLong catchUpResets = new AtomicLong(0);
        final AtomicLong skippedCatchUpRecords = new AtomicLong(0);
        final AtomicLong maxScheduleLagNs = new AtomicLong(0);
        final AtomicLong[] sendBuckets;
        final AtomicLong[] ackBuckets;
        volatile Throwable startupError;

        public ProducerTask(int id, int opsPerSec, byte[] payload,
                            AtomicBoolean running,
                            CountDownLatch readyLatch,
                            CountDownLatch startLatch,
                            CountDownLatch doneLatch,
                            int capacity, Semaphore outstandingLimiter) {
            this.id = id;
            this.opsPerSec = opsPerSec;
            this.payload = payload;
            this.running = running;
            this.readyLatch = readyLatch;
            this.startLatch = startLatch;
            this.doneLatch = doneLatch;
            this.latenciesNs = new long[capacity];
            this.ackSecond = new int[capacity];
            this.outstandingLimiter = outstandingLimiter;
            int bucketCount = Math.addExact(warmupSec, measureSec);
            this.sendBuckets = atomicBuckets(bucketCount);
            this.ackBuckets = atomicBuckets(bucketCount);
        }

        @Override
        public void run() {
            // Producer 하나마다 독립 KafkaProducer와 sender thread가 만들어진다.
            // client.id를 분리해 서버/client metric에서 producer별 상태를 식별한다.
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "bench-producer-" + id);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringSerializer");
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.ByteArraySerializer");

            // acks=1은 leader가 local log append를 처리한 뒤 응답하지만 fsync 완료를
            // 의미하지 않는다. 따라서 callback latency를 durable disk latency로
            // 해석하면 안 된다.
            props.put(ProducerConfig.ACKS_CONFIG, "1");

            // batch.size를 record 하나가 들어갈 정도로 제한하고 linger.ms=0을 사용한다.
            // batching을 완전히 금지하는 설정은 아니지만 여러 record가 오래 대기하며
            // 큰 batch를 만드는 효과를 최소화한다.
            int batchSize = recordSize + 512;
            props.put(ProducerConfig.LINGER_MS_CONFIG, "0");
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(batchSize));
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

            // 과부하 시 request timeout보다 delivery timeout이 길어야 한다. callback에
            // 전달되는 timeout도 Failed Requests로 집계된다.
            props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "60000");
            props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");
            props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "10485760");

            // 비동기 send는 broker보다 빠르게 호출되면 producer buffer를 채운다.
            // 큰 record일수록 최소한의 outstanding 데이터를 담을 수 있도록 buffer를
            // 키우되 producer 수를 곱한 총 JVM heap 사용량을 함께 고려해야 한다.
            long bufferPerProducer;
            if (recordSize <= 10 * 1024) {
                bufferPerProducer = 16L * 1024 * 1024;
            } else if (recordSize <= 100 * 1024) {
                bufferPerProducer = 32L * 1024 * 1024;
            } else {
                bufferPerProducer = 64L * 1024 * 1024;
            }
            props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(bufferPerProducer));
            props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000");
            if (id == 0) {
                System.out.printf("[Config] BUFFER_MEMORY per producer = %d MB%n",
                        bufferPerProducer / (1024 * 1024));
                System.out.printf("[Config] linger.ms=0, batch.size=%d%n", batchSize);
            }

            long intervalNs = opsPerSec == 0 ? Long.MAX_VALUE
                    : Math.max(1L, 1_000_000_000L / opsPerSec);
            KafkaProducer<String, byte[]> producer = null;
            boolean readySignalled = false;
            try {
                // KafkaProducer 생성 실패도 main thread에 전달해야 하므로 readyLatch는
                // 성공과 실패 경로 모두에서 정확히 한 번 감소시킨다.
                producer = new KafkaProducer<>(props);
                readyLatch.countDown();
                readySignalled = true;
                startLatch.await();
                if (opsPerSec == 0) return;

                long nextSendNs = System.nanoTime();
                while (running.get() && System.nanoTime() < measureEndNs) {
                    // 예정 시각보다 지나치게 늦으면 밀린 요청을 연속 전송하지 않고
                    // schedule을 현재 시각으로 옮긴다. 두 제한이 0이면 reset하지 않는다.
                    long lagNs = System.nanoTime() - nextSendNs;
                    if (lagNs > 0) {
                        updateMax(maxScheduleLagNs, lagNs);
                        long recordLimitNs = maxCatchUpRecords > 0
                                ? saturatedMultiply(intervalNs, maxCatchUpRecords) : Long.MAX_VALUE;
                        long timeLimitNs = maxScheduleLagMs > 0
                                ? TimeUnit.MILLISECONDS.toNanos(maxScheduleLagMs) : Long.MAX_VALUE;
                        if (lagNs > Math.min(recordLimitNs, timeLimitNs)) {
                            catchUpResets.incrementAndGet();
                            skippedCatchUpRecords.addAndGet(Math.max(1, lagNs / intervalNs));
                            nextSendNs = System.nanoTime();
                        }
                    }
                    while (running.get()) {
                        // 긴 구간은 park로 CPU를 양보하고 마지막 100us 부근은 spin한다.
                        // 높은 target에서는 정확도가 좋아지는 대신 CPU 사용량이 증가한다.
                        long remainingNs = nextSendNs - System.nanoTime();
                        if (remainingNs <= 0) break;
                        if (remainingNs > 200_000L) {
                            LockSupport.parkNanos(remainingNs - 100_000L);
                        } else {
                            Thread.onSpinWait();
                        }
                    }
                    if (!running.get() || System.nanoTime() >= measureEndNs) break;

                    boolean permitAcquired = false;
                    if (outstandingLimiter != null) {
                        // 이 semaphore는 Kafka의 max.in.flight.requests와 다르다. 모든
                        // producer가 send한 뒤 callback을 기다리는 record 총수를 제한한다.
                        long waitStart = System.nanoTime();
                        if (!outstandingLimiter.tryAcquire()) {
                            limiterWaitCount.incrementAndGet();
                            outstandingLimiter.acquire();
                            limiterWaitNs.addAndGet(System.nanoTime() - waitStart);
                        }
                        permitAcquired = true;
                    }
                    if (!running.get() || System.nanoTime() >= measureEndNs) {
                        if (permitAcquired) outstandingLimiter.release();
                        break;
                    }

                    ProducerRecord<String, byte[]> record = new ProducerRecord<>(topicName, payload);
                    final long sendStart = System.nanoTime();
                    final int sendSecond = elapsedSecond(sendStart);
                    if (sendSecond >= 0 && sendSecond < sendBuckets.length) {
                        sendBuckets[sendSecond].incrementAndGet();
                    }
                    final boolean measured = sendStart >= measureStartNs
                            && sendStart < measureEndNs;
                    final boolean releasePermit = permitAcquired;

                    // send 호출 전에 outstanding을 증가시켜 매우 빠른 callback이 먼저
                    // 실행되더라도 count가 음수가 되지 않게 한다. 동기 예외 경로에서는
                    // 아래 catch가 outstanding과 permit을 원상 복구한다.
                    long outstanding = globalOutstanding.incrementAndGet();
                    updateMax(globalMaxOutstanding, outstanding);
                    if (measured) {
                        measuredSent.incrementAndGet();
                        measuredOutstanding.incrementAndGet();
                    }
                    try {
                        producer.send(record, (metadata, exception) -> {
                            // callback은 Kafka sender thread에서 실행된다. 성공/실패와
                            // 관계없이 outstanding 및 semaphore permit을 먼저 정리한다.
                            long callbackNs = System.nanoTime();
                            if (releasePermit) outstandingLimiter.release();
                            globalOutstanding.decrementAndGet();
                            if (measured) measuredOutstanding.decrementAndGet();
                            if (exception != null) {
                                sendErrors.incrementAndGet();
                                if (measured) measuredFailures.incrementAndGet();
                                return;
                            }
                            int sec = elapsedSecond(callbackNs);
                            long latency = callbackNs - sendStart;
                            totalSent.incrementAndGet();
                            if (sec >= 0 && sec < ackBuckets.length) {
                                ackBuckets[sec].incrementAndGet();
                            }
                            if (measured) {
                                // ACK Window는 measurement 안의 완료량, Eventual ACK는
                                // drain까지 포함한 최종 완료량이다. latency는 두 경우 모두
                                // 원래 sendStart부터 callback까지 전체 시간을 보존한다.
                                measuredEventualAck.incrementAndGet();
                                if (callbackNs < measureEndNs) measuredAckInWindow.incrementAndGet();
                                int idx = recordedCount.get();
                                if (idx < latenciesNs.length) {
                                    latenciesNs[idx] = latency;
                                    ackSecond[idx] = sec;
                                    recordedCount.incrementAndGet();
                                } else {
                                    droppedLatencySamples.incrementAndGet();
                                }
                            }
                        });
                    } catch (Exception e) {
                        sendErrors.incrementAndGet();
                        if (permitAcquired) outstandingLimiter.release();
                        globalOutstanding.decrementAndGet();
                        if (measured) {
                            measuredOutstanding.decrementAndGet();
                            measuredFailures.incrementAndGet();
                        }
                    }
                    // 실제 완료 시각이 아니라 이전 예정 시각을 기준으로 다음 deadline을
                    // 계산해 scheduler wake-up 오차가 매 요청마다 누적되지 않게 한다.
                    nextSendNs += intervalNs;
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                if (!readySignalled) startupError = e;
                System.err.println("[Producer-" + id + "] Fatal: " + e);
                e.printStackTrace(System.err);
            } finally {
                if (!readySignalled) readyLatch.countDown();
                if (producer != null) {
                    // flush에서 제출된 record의 callback을 기다린다. main thread의 drain
                    // timeout이 먼저 끝나면 이 thread가 아직 실행 중일 수 있으므로 해당
                    // 결과는 Drain Completed=false로 무효 처리해야 한다.
                    try { producer.flush(); } catch (Exception ignore) {}
                    try { producer.close(Duration.ofSeconds(30)); } catch (Exception ignore) {}
                }
                doneLatch.countDown();
            }
        }

        private int elapsedSecond(long nowNs) {
            long elapsedNs = nowNs - globalStartNs;
            if (elapsedNs < 0) return -1;
            long sec = TimeUnit.NANOSECONDS.toSeconds(elapsedNs);
            return sec > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) sec;
        }
    }

    /**
     * Scenario B에서 단일 consumer의 순차 read 부하를 추가한다.
     * 매 실행마다 새 group을 사용하고 earliest부터 읽으므로 main topic을 실행 전에
     * 재생성하지 않으면 과거 데이터까지 읽어 비교 조건이 달라질 수 있다.
     */
    static class ConsumerTask implements Runnable {
        private final AtomicBoolean running;

        public ConsumerTask(AtomicBoolean running) {
            this.running = running;
        }

        @Override
        public void run() {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

            // 매 run에 새 group을 사용하므로 committed offset을 재사용하지 않는다.
            // auto.offset.reset=earliest와 결합되어 현재 topic의 처음부터 읽는다.
            // 실행기가 main topic을 매번 재생성해야 동일한 데이터 범위를 보장할 수 있다.
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "bench-group-" + UUID.randomUUID());
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // 이 consumer는 read I/O 부하 생성용이며 결과를 재처리하지 않는다.
            // offset commit을 끄면 commit I/O와 __consumer_offsets 부하가 섞이지 않는다.
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

            // broker가 가능하면 1MiB 가까이 모아 응답하게 해 작은 fetch 남발을 줄인다.
            // max.partition.fetch.bytes는 단일 record보다 충분히 커야 하며 record size를
            // 늘릴 경우 producer/broker size 설정과 함께 검토해야 한다.
            props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1048576");
            props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "1048576");

            try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
                // subscribe 후 실제 partition assignment는 첫 poll에서 수행된다.
                consumer.subscribe(Collections.singletonList(topicName));
                long consumed = 0;
                while (running.get()) {
                    // seek를 반복하지 않고 현재 position에서 순차 consume한다. 따라서
                    // 같은 record를 반복해서 읽는 인위적인 read amplification이 없다.
                    ConsumerRecords<String, byte[]> records =
                            consumer.poll(Duration.ofMillis(100));
                    consumed += records.count();
                }
                // running=false 뒤 추가 poll은 하지 않는다. 출력값은 consumer가 실제로
                // 반환받은 record 수이며 producer ACK 수와 반드시 같지는 않다.
                System.out.printf("[Consumer] consumed %d records%n", consumed);
            } catch (Exception e) {
                System.err.println("[Consumer] error: " + e);
            }
        }
    }

    /**
     * 1-partition topic을 지정 속도로 생성해 KRaft metadata 부하를 추가한다.
     * baseline filesystem 결과와 직접 합치지 말고 별도 시나리오로 비교해야 한다.
     */
    static class TopicCreatorTask implements Runnable {
        private final AtomicBoolean running;
        private final int ratePerSec;
        private final long sleepMsBetween;

        public TopicCreatorTask(AtomicBoolean running, int ratePerSec) {
            this.running = running;
            this.ratePerSec = Math.max(1, ratePerSec);
            this.sleepMsBetween = Math.max(1L, 1000L / this.ratePerSec);
        }

        @Override
        public void run() {
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            int created = 0;
            int failed = 0;
            String firstFailure = "none";
            long firstFailureElapsedMs = -1;
            String runPrefix = UUID.randomUUID().toString().substring(0, 8);
            long taskStartNs = System.nanoTime();
            try (AdminClient admin = AdminClient.create(props)) {
                int counter = 0;
                while (running.get()) {
                    String dTopic = "dyn-topic-" + runPrefix + "-" + (counter++);
                    try {
                        NewTopic newTopic = new NewTopic(dTopic, 1, (short) 1);
                        admin.createTopics(Collections.singletonList(newTopic))
                             .all().get(5, TimeUnit.SECONDS);
                        created++;
                    } catch (Exception e) {
                        failed++;
                        if (firstFailureElapsedMs < 0) {
                            firstFailureElapsedMs = TimeUnit.NANOSECONDS.toMillis(
                                    System.nanoTime() - taskStartNs);
                            Throwable cause = e.getCause() == null ? e : e.getCause();
                            firstFailure = cause.getClass().getSimpleName() + ": "
                                    + String.valueOf(cause.getMessage());
                        }
                        if (failed <= 5) {
                            System.err.println("[TopicCreator] create failed: " + e.getMessage());
                        }
                    }
                    try {
                        Thread.sleep(sleepMsBetween);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
            System.out.printf("[TopicCreator] created=%d failed=%d (rate=%d/sec)%n",
                    created, failed, ratePerSec);
            System.out.printf("[TopicCreator] first_failure_elapsed_ms=%d first_failure=%s%n",
                    firstFailureElapsedMs, firstFailure);
        }
    }

    /**
     * {@code --option value} 형식만 허용한다. 잘못된 값을 묵인하면 실험 조건과
     * 보고서가 달라질 수 있으므로 알 수 없는 옵션과 느슨한 boolean을 거부한다.
     */
    private static void parseArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String option = args[i];
            String value = requireValue(args, ++i, option);
            switch (option) {
                case "--record-size":        recordSize = Integer.parseInt(value); break;
                case "--target-ops":         targetOps = Integer.parseInt(value); break;
                case "--producers":          numProducers = Integer.parseInt(value); break;
                case "--use-consumer":       useConsumer = parseBoolean(value, option); break;
                case "--dynamic-topics":     dynamicTopicCreation = parseBoolean(value, option); break;
                case "--warmup-sec":         warmupSec = Integer.parseInt(value); break;
                case "--measure-sec":        measureSec = Integer.parseInt(value); break;
                case "--drain-timeout-sec":  drainTimeoutSec = Integer.parseInt(value); break;
                // 과거 실행기 호환 alias. 신규 실행기는 --measure-sec를 사용한다.
                case "--duration":           measureSec = Integer.parseInt(value); break;
                case "--dynamic-topic-rate": dynamicTopicRate = Integer.parseInt(value); break;
                case "--bootstrap-servers":  bootstrapServers = value; break;
                case "--topic":              topicName = value; break;
                case "--max-in-flight-records": maxInFlightRecords = Integer.parseInt(value); break;
                case "--max-catch-up-records": maxCatchUpRecords = Integer.parseInt(value); break;
                case "--max-schedule-lag-ms": maxScheduleLagMs = Long.parseLong(value); break;
                case "--producer-start-timeout-sec":
                    producerStartTimeoutSec = Integer.parseInt(value); break;
                default: throw new IllegalArgumentException("Unknown option: " + option);
            }
        }
    }

    private static String requireValue(String[] args, int index, String option) {
        if (index >= args.length) {
            throw new IllegalArgumentException("Missing value for " + option);
        }
        return args[index];
    }

    private static boolean parseBoolean(String value, String option) {
        if ("true".equalsIgnoreCase(value)) return true;
        if ("false".equalsIgnoreCase(value)) return false;
        throw new IllegalArgumentException(option + " must be true or false");
    }
}
