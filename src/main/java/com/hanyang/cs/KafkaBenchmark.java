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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
 *   - 고정된 Target OP/s 부하 하에서 파일시스템별 응답 지연(latency) 측정.
 *   - Kafka 4.2 / KRaft 단일 브로커 / replication=1 환경.
 *   - ext4(CNS) vs f2fs(CNS) 베이스라인 → 이후 Milestone #2(FEMU + ZNS)와 비교.
 *
 *  Two-level latency 분석 전략
 *   이 벤치마크가 측정하는 latency 는 "broker 의 OS 페이지 캐시까지 도달한 시점"임.
 *   Kafka 의 acks=1 ACK 시점은 .log 파일에 write() 시스템 콜이 완료된 시점이고,
 *   이는 OS 페이지 캐시까지 도달한 것이지 디스크 platter 까지는 아님.
 *
 *   따라서 보고서에서는 두 layer 를 분리해 분석:
 *     - App-level latency  : 이 코드가 측정하는 send→ACK 시간 (Kafka + 페이지 캐시 layer)
 *     - Block-level latency: iostat 의 await (실제 디스크 I/O queue 처리 시간)
 *   두 값의 차이로 페이지 캐시의 latency 흡수 효과까지 보일 수 있음.
 *
 *  비동기 콜백 send 사용
 *   - producer.send(record, callback) 형태. producer 스레드는 send 호출 후 즉시 다음 진행.
 *   - latency 는 콜백에서 (System.nanoTime() - sendStart) 로 측정.
 *     → 측정 시점이 ACK 받은 시점이므로 send→ACK 의 진짜 latency.
 *   - 동기 send (.get()) 보다 훨씬 높은 throughput 가능 → "디스크 부하 충분히 형성" 조건 만족.
 *   - 콜백은 Kafka 의 sender thread 에서 호출되므로 측정용 자료구조는 thread-safe 해야 함.
 *
 *  측정 신뢰성 관련 설계
 *   1) producer 마다 자기 long[] 에 latency 기록 → 공유 List/synchronized 제거.
 *      단, 콜백이 다른 스레드(=Kafka sender thread) 에서 실행되므로 인덱스는 AtomicInteger.
 *   2) CountDownLatch 로 모든 producer 동시 출발 → warmup 윈도우 일관성.
 *   3) 종료 시 running=false → producer 스레드는 send 멈춤 → producer.flush() 로 in-flight
 *      콜백을 모두 처리한 후 통계 집계. 통계에는 ACK 받은 record 만 들어감.
 *
 *  메모리 안전성 (5.8GB RAM 환경 가정)
 *   - BUFFER_MEMORY 를 record size 에 따라 동적으로 조정 → OOM 방지.
 *   - 1KB/10KB → 16MB, 100KB → 32MB, 1MB → 64MB.
 *   - producer 수는 Python 자동화 스크립트에서 record size 별로 다르게 부여.
 * =============================================================================
 */
public class KafkaBenchmark {

    // -----------------------------------------------------------------------
    // 브로커 접속 정보
    // -----------------------------------------------------------------------
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC_NAME = "bench-topic";


    // 임시 추가
    static final int MAX_SEC = 600;

    static AtomicLong[] sendBuckets = new AtomicLong[MAX_SEC];
    static AtomicLong[] ackBuckets  = new AtomicLong[MAX_SEC];

    static long[][] latencyBuckets = new long[MAX_SEC][];
    static AtomicInteger[] latencyCounts = new AtomicInteger[MAX_SEC];

    static {
        for (int i = 0; i < MAX_SEC; i++) {
            sendBuckets[i] = new AtomicLong(0);
            ackBuckets[i] = new AtomicLong(0);

            latencyBuckets[i] = new long[100000]; // 필요시 조정
            latencyCounts[i] = new AtomicInteger(0);
        }
    }
    //

    // -----------------------------------------------------------------------
    // CLI 인자로 채워지는 실행 파라미터 (default 값들은 단독 실행 시 fallback)
    // -----------------------------------------------------------------------
    private static int recordSize = 1024;
    private static int targetOps = 10000;
    private static int numProducers = 16;
    private static boolean useConsumer = false;
    private static boolean dynamicTopicCreation = false;
    private static int durationSec = 60;
    private static int warmupSec = 0;
    private static int dynamicTopicRate = 1;

    public static void main(String[] args) throws Exception {
        parseArgs(args);

        System.out.println("=====================================");
        System.out.println(" Kafka Filesystem Benchmark (Java API) ");
        System.out.println("=====================================");
        System.out.printf(" - Record Size: %d Bytes%n", recordSize);
        System.out.printf(" - Target OP/s (total): %d%n", targetOps);
        System.out.printf(" - Producers: %d%n", numProducers);
        System.out.printf(" - Consumer Active: %b%n", useConsumer);
        System.out.printf(" - Dynamic Topic Creation: %b (rate=%d/sec)%n",
                dynamicTopicCreation, dynamicTopicRate);
        System.out.printf(" - Warmup: %d sec / Total Duration: %d sec%n",
                warmupSec, durationSec);
        System.out.println(" - Send mode: ASYNC (callback-based latency measurement)");

        AtomicBoolean running = new AtomicBoolean(true);
        CountDownLatch readyLatch = new CountDownLatch(numProducers);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numProducers);

        byte[] payload = new byte[recordSize];
        new Random(42).nextBytes(payload);

        int opsPerProducer = Math.max(1, targetOps / numProducers);

        // 비동기 send 라 producer 한 개가 큐에 많이 쌓을 수 있음.
        // 2.0x 여유로 latency 배열 크기 결정 (capacity 초과는 silently drop).
        int perProducerCapacity = Math.max(2048, (int) (opsPerProducer * durationSec * 2.0));

        ProducerTask[] tasks = new ProducerTask[numProducers];
        ExecutorService executor = Executors.newFixedThreadPool(
                numProducers + (useConsumer ? 1 : 0) + (dynamicTopicCreation ? 1 : 0));

        for (int i = 0; i < numProducers; i++) {
            tasks[i] = new ProducerTask(
                    i, opsPerProducer, payload, running,
                    readyLatch, startLatch, doneLatch,
                    perProducerCapacity);
            executor.submit(tasks[i]);
        }

        ConsumerTask consumerTask = null;
        if (useConsumer) {
            consumerTask = new ConsumerTask(running);
            executor.submit(consumerTask);
        }

        TopicCreatorTask topicTask = null;
        if (dynamicTopicCreation) {
            topicTask = new TopicCreatorTask(running, dynamicTopicRate);
            executor.submit(topicTask);
        }
        if (consumerTask == null && topicTask == null) {
            // unused warning silencer
        }

        readyLatch.await();
        long startTime = System.currentTimeMillis();
        ProducerTask.globalStartTime = startTime;
        startLatch.countDown();

        System.out.printf("[Run] Producers started. warmup=%ds, total=%ds%n",
                warmupSec, durationSec);

        Thread.sleep(durationSec * 1000L);
        running.set(false);

        // 비동기라 producer 의 send loop 가 멈춰도 in-flight 콜백이 남아 있음.
        // doneLatch 는 ProducerTask 가 producer.close() 를 마친 후에 countDown 함.
        // close() 내부에서 flush() 가 호출되므로 모든 in-flight callback 이 실행된 후 doneLatch 깨어남.
        // 180초 cap: broker 응답이 늦어지는 극단 케이스 안전망.
        boolean cleanFinish = doneLatch.await(180, TimeUnit.SECONDS);
        if (!cleanFinish) {
            System.err.println("[Warn] Some producers did not finish in 180s. Forcing shutdown.");
        }

        executor.shutdownNow();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        printMetrics(tasks, startTime);
    }

    /**
     * producer 별 latency 배열을 모아 정렬 후 통계 출력.
     *
     * Python 스크립트의 parse_java_metrics() 가 다음 정규식들로 파싱:
     *   "Total Requests : %d" / "Average : %.2f ms" / "p50 / p90 / p99 / p999 / Max"
     *   "Achieved OP/s : %.2f" / "Achieved/Target (%) : %.1f"
     *   "Total Sent (incl. warmup) : %d" / "Send Errors : %d"
     * 출력 포맷을 바꾸면 Python 파서도 함께 수정 필요.
     */
    private static void printMetrics(ProducerTask[] tasks, long globalStartMs) {
        long totalSamples = 0;
        long totalSentIncludingWarmup = 0;
        long totalSendErrors = 0;
        for (ProducerTask t : tasks) {
            // 비동기라 recordedCount 가 AtomicInteger.
            int n = t.recordedCount.get();
            totalSamples += n;
            totalSentIncludingWarmup += t.totalSent.get();
            totalSendErrors += t.sendErrors.get();
        }

        if (totalSamples == 0) {
            System.out.println("\n--- [ Latency Results (ms) ] ---");
            System.out.println(" No samples recorded after warmup.");
            System.out.printf(" Total Sent (incl. warmup) : %d%n", totalSentIncludingWarmup);
            System.out.printf(" Send Errors               : %d%n", totalSendErrors);
            System.out.println("--------------------------------");
            return;
        }

        long[] all = new long[(int) totalSamples];
        int idx = 0;
        for (ProducerTask t : tasks) {
            int n = t.recordedCount.get();
            // capacity 를 넘은 만큼은 latenciesNs 에 안 들어가 있으므로
            // recordedCount 를 latenciesNs.length 로 clamp.
            n = Math.min(n, t.latenciesNs.length);
            System.arraycopy(t.latenciesNs, 0, all, idx, n);
            idx += n;
        }
        // 위에서 clamp 했기 때문에 idx 와 totalSamples 가 살짝 다를 수 있음 → 실제 채워진 만큼만 정렬.
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

        long elapsedMs = System.currentTimeMillis() - globalStartMs;
        long measureWindowMs = Math.max(1L, elapsedMs - warmupSec * 1000L);
        double achievedOpsPerSec = (totalSamples * 1000.0) / measureWindowMs;

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
        System.out.println("--------------------------------");
        System.out.println("\n--- [ Per-second Throughput & Latency ] ---");

        for (int sec = 0; sec < durationSec; sec++) {
            long send = sendBuckets[sec].get();
            long ack  = ackBuckets[sec].get();

            int count = latencyCounts[sec].get();

            if (count == 0) {
                System.out.printf("Sec %3d | send=%6d | ack=%6d | no data\n",
                        sec, send, ack);
                continue;
            }

            count = Math.min(count, latencyBuckets[sec].length);
            long[] arr = Arrays.copyOf(latencyBuckets[sec], count);
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

    // ==========================================================================
    // ProducerTask
    //   - 비동기 send + 콜백에서 latency 기록.
    //   - 콜백은 Kafka 의 sender thread 에서 호출되므로:
    //       recordedCount: AtomicInteger (콜백 vs main 스레드 visibility 보장)
    //       latenciesNs[]: long[] 자체는 race 없이 idx 다른 위치에 write
    //       totalSent / sendErrors: 이미 AtomicLong
    // ==========================================================================
    static class ProducerTask implements Runnable {
        // 모든 producer 가 공유하는 글로벌 시작 시각 (warmup 컷오프 판정용).
        static volatile long globalStartTime = 0L;

        private final int id;
        private final int opsPerSec;
        private final byte[] payload;
        private final AtomicBoolean running;
        private final CountDownLatch readyLatch;
        private final CountDownLatch startLatch;
        private final CountDownLatch doneLatch;

        // 측정 결과:
        //   비동기 콜백이 latenciesNs 의 다른 인덱스에 동시에 쓰는 형태.
        //   AtomicInteger.getAndIncrement() 로 인덱스 충돌 없이 분배.
        //   capacity 초과 시 그 콜백은 latency 기록 skip (totalSent 에는 카운트됨).
        final long[] latenciesNs;
        final AtomicInteger recordedCount = new AtomicInteger(0);

        // 디버깅/검증용:
        final AtomicLong totalSent = new AtomicLong(0);
        final AtomicLong sendErrors = new AtomicLong(0);

        public ProducerTask(int id, int opsPerSec, byte[] payload,
                            AtomicBoolean running,
                            CountDownLatch readyLatch,
                            CountDownLatch startLatch,
                            CountDownLatch doneLatch,
                            int capacity) {
            this.id = id;
            this.opsPerSec = Math.max(1, opsPerSec);
            this.payload = payload;
            this.running = running;
            this.readyLatch = readyLatch;
            this.startLatch = startLatch;
            this.doneLatch = doneLatch;
            this.latenciesNs = new long[capacity];
        }

        @Override
        public void run() {
            // ===============================================================
            // KafkaProducer 설정
            // ===============================================================
            Properties props = new Properties();

            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "bench-producer-" + id);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringSerializer");
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.ByteArraySerializer");

            // -----------------------------------------------------------
            // ACKS = 1
            //   leader broker 가 .log 파일에 write() 시스템 콜 완료 후 ACK.
            //   = OS 페이지 캐시까지 도달 시점. 디스크 platter 까지는 아님 (그건 iostat 으로 측정).
            //   단일 브로커 + replication=1 이라 acks=all 과 동일 동작이지만 의미 명확화 위해 1 명시.
            // -----------------------------------------------------------
            props.put(ProducerConfig.ACKS_CONFIG, "1");

            // -----------------------------------------------------------
            // BATCHING: application-level batching 최소화
            //   LINGER_MS=5 + BATCH_SIZE=16KB → 적당한 batching 으로 throughput 확보,
            //   동시에 broker 입장에서 자잘한 write 가 충분히 들어와 fs write pattern 차이 부각.
            // -----------------------------------------------------------
            props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");

            // -----------------------------------------------------------
            // MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = 5
            //   비동기 send + 5 in-flight pipelining → producer 한 개의 throughput 향상.
            //   동기 send (=1) 대비 ~3~5배 throughput 가능 → "디스크 부하 충분히 형성" 조건 만족.
            //   측정값에 약간의 pipelining 분산 섞이지만 fs 비교 목적엔 영향 미미.
            //   단, 5 이하여야 idempotent producer 보장. 5 = Kafka 권장 default.
            // -----------------------------------------------------------
            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

            // -----------------------------------------------------------
            // TIMEOUT 들 (큰 record + 디스크 폭주 시 timeout 방지)
            // -----------------------------------------------------------
            props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "60000");
            props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");

            // -----------------------------------------------------------
            // 큰 record 처리용 size 들
            //   MAX_REQUEST_SIZE: 1000KB record 가 헤더 포함 1MB 살짝 넘을 수 있어 10MB.
            // -----------------------------------------------------------
            props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "10485760");

            // -----------------------------------------------------------
            // BUFFER_MEMORY 동적 조정 (OOM 방지 — 5.8GB RAM 환경)
            //   record size 별로 다르게 잡아서 producer × buffer 총량을 안전 범위로 유지.
            //   1KB,10KB → 16MB,  100KB → 32MB,  1MB → 64MB.
            //   비동기 send 라 buffer 가 빠르게 차오를 수 있어 record 크기에 비례하는 게 맞음.
            // -----------------------------------------------------------
            long bufferPerProducer;
            if (recordSize <= 10 * 1024) {
                bufferPerProducer = 16L * 1024 * 1024;       // 1KB, 10KB → 16MB
            } else if (recordSize <= 100 * 1024) {
                bufferPerProducer = 32L * 1024 * 1024;       // 100KB → 32MB
            } else {
                bufferPerProducer = 64L * 1024 * 1024;       // 1MB → 64MB
            }
            props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(bufferPerProducer));
            if (id == 0) {
                System.out.printf("[Config] BUFFER_MEMORY per producer = %d MB%n",
                        bufferPerProducer / (1024 * 1024));
            }

            // Throttle 간격
            long intervalNs = 1_000_000_000L / opsPerSec;

            KafkaProducer<String, byte[]> producer = null;
            try {
                producer = new KafkaProducer<>(props);
                readyLatch.countDown();
                startLatch.await();

                // -------------------------------------------------------
                // 메인 send 루프 (비동기)
                // -------------------------------------------------------
                while (running.get()) {
                    long loopStart = System.nanoTime();

                    ProducerRecord<String, byte[]> record =
                            new ProducerRecord<>(TOPIC_NAME, payload);

                    // 임시 추가
                    long nowMs = System.currentTimeMillis();
                    long elapsedSec = (nowMs - globalStartTime) / 1000;

                    if (elapsedSec < MAX_SEC) {
                        sendBuckets[(int) elapsedSec].incrementAndGet();
                    }           
                    //


                    // 람다에서 캡쳐하기 위해 final 지역 변수.
                    // 매 send 마다 새 sendStart 가 캡쳐되어야 하므로 람다 안에서 측정 시점 기록.
                    final long sendStart = System.nanoTime();
                    try {
                        // ★ 비동기 send + 콜백
                        //
                        //   producer.send(record, callback):
                        //     - record 를 producer 내부 큐에 push (즉시 리턴, non-blocking)
                        //     - background sender thread 가 broker 로 보내고 ACK 받으면 callback 호출
                        //     - callback 에서 측정한 latency = sendStart 시점부터 ACK 시점까지의 시간
                        //
                        //   콜백 실행 스레드: Kafka sender thread (producer 마다 1개).
                        //     - 같은 ProducerTask 내의 모든 콜백은 같은 sender thread 에서 순차 실행됨.
                        //     - 따라서 같은 ProducerTask 의 recordedCount/latenciesNs 접근은 순차적.
                        //     - 그러나 main 스레드에서 totalSamples 집계 시 visibility 보장이 필요해
                        //       AtomicInteger 사용.
                        producer.send(record, (metadata, exception) -> {
                            if (exception != null) {
                                sendErrors.incrementAndGet();
                                return;
                            }

                            long now = System.currentTimeMillis();
                            long sec = (now - globalStartTime) / 1000;

                            long latency = System.nanoTime() - sendStart;
                            totalSent.incrementAndGet();

                            // ==================== ADD HERE ====================
                            if (sec < MAX_SEC) {
                                // ACK throughput
                                ackBuckets[(int) sec].incrementAndGet();

                                // latency per-second 저장
                                int idx = latencyCounts[(int) sec].getAndIncrement();
                                if (idx < latencyBuckets[(int) sec].length) {
                                    latencyBuckets[(int) sec][idx] = latency;
                                }
                            }
                            // =================================================

                            // 기존 warmup 필터 (그대로 유지)
                            if (now - globalStartTime > warmupSec * 1000L) {
                                int idx = recordedCount.getAndIncrement();
                                if (idx < latenciesNs.length) {
                                    latenciesNs[idx] = latency;
                                }
                            }
                        });
                    } catch (Exception e) {
                        // send 자체가 실패한 경우 (BUFFER 가득 차서 block 후 timeout 등)
                        // 콜백이 호출되지 않으므로 여기서 직접 errors 카운트.
                        sendErrors.incrementAndGet();
                    }

                    // OP/s throttle
                    long elapsed = System.nanoTime() - loopStart;
                    if (elapsed < intervalNs) {
                        long sleepNs = intervalNs - elapsed;
                        long ms = sleepNs / 1_000_000L;
                        int  ns = (int) (sleepNs % 1_000_000L);
                        try {
                            if (ms > 0 || ns > 0) Thread.sleep(ms, ns);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.err.println("[Producer-" + id + "] Fatal: " + e);
                e.printStackTrace(System.err);
            } finally {
                // -------------------------------------------------------
                // 종료 시퀀스 (비동기 send 의 in-flight 콜백 마무리가 핵심)
                // -------------------------------------------------------
                if (producer != null) {
                    // flush(): producer 큐의 모든 record 를 broker 로 보내고 ACK 모두 받을 때까지 block.
                    //          비동기 send 의 in-flight 콜백들이 여기서 다 실행됨.
                    //          → 통계에 마지막까지 ACK 받은 record 가 모두 반영되도록 보장.
                    try { producer.flush(); } catch (Exception ignore) {}

                    // close(timeout): 추가로 connection 정리.
                    //   timeout 안에 못 끝나면 강제 종료 (broker hang 시 안전망).
                    try { producer.close(Duration.ofSeconds(30)); } catch (Exception ignore) {}
                }
                doneLatch.countDown();
            }
        }
    }

    // ==========================================================================
    // ConsumerTask (Scenario B 전용)
    //   - 정상 consume 만 수행. seekToBeginning 무한 호출 제거.
    //     (이전 구현은 consumer 단독으로 producer 의 16배 read 부하를 일으켜
    //      "Producer + Consumer 1개" 시나리오 의도를 왜곡함)
    //   - 단순히 throughput 따라잡으며 read I/O 발생시키는 역할.
    // ==========================================================================
    static class ConsumerTask implements Runnable {
        private final AtomicBoolean running;

        public ConsumerTask(AtomicBoolean running) {
            this.running = running;
        }

        @Override
        public void run() {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "bench-group-" + UUID.randomUUID());
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1048576");
            props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "1048576");

            try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(Collections.singletonList(TOPIC_NAME));
                long consumed = 0;
                while (running.get()) {
                    ConsumerRecords<String, byte[]> records =
                            consumer.poll(Duration.ofMillis(100));
                    consumed += records.count();
                }
                System.out.printf("[Consumer] consumed %d records%n", consumed);
            } catch (Exception e) {
                System.err.println("[Consumer] error: " + e);
            }
        }
    }

    // ==========================================================================
    // TopicCreatorTask (Optional: 동적 토픽 생성 부하)
    //   - 메타데이터 갱신 부하 측정용. partition=1 로 가벼운 토픽.
    // ==========================================================================
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
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            int created = 0;
            int failed = 0;
            try (AdminClient admin = AdminClient.create(props)) {
                int counter = 0;
                while (running.get()) {
                    String dTopic = "dyn-topic-" + (counter++);
                    try {
                        NewTopic newTopic = new NewTopic(dTopic, 1, (short) 1);
                        admin.createTopics(Collections.singletonList(newTopic))
                             .all().get(5, TimeUnit.SECONDS);
                        created++;
                    } catch (Exception e) {
                        failed++;
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
        }
    }

    private static void parseArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--record-size":        recordSize = Integer.parseInt(args[++i]); break;
                case "--target-ops":         targetOps = Integer.parseInt(args[++i]); break;
                case "--producers":          numProducers = Integer.parseInt(args[++i]); break;
                case "--use-consumer":       useConsumer = Boolean.parseBoolean(args[++i]); break;
                case "--dynamic-topics":     dynamicTopicCreation = Boolean.parseBoolean(args[++i]); break;
                case "--duration":           durationSec = Integer.parseInt(args[++i]); break;
                case "--warmup-sec":         warmupSec = Integer.parseInt(args[++i]); break;
                case "--dynamic-topic-rate": dynamicTopicRate = Integer.parseInt(args[++i]); break;
                default:
                    System.err.println("[Warn] Unknown arg: " + args[i]);
            }
        }
    }
}