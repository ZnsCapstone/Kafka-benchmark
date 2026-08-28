# KafkaBenchmark 수정 기록

##  고빈도 OP/s pacing 개선

### 수정 이유

기존 producer loop는 매 요청 뒤에 `Thread.sleep(ms, ns)`로 다음 요청까지
대기했다. 1KB, 목표 100,000 OP/s, producer 8개 조건에서는 producer 하나당
12,500 OP/s가 필요하므로 요청 간격이 약 80µs다. 일반 Linux 스케줄러에서
이 정도의 `Thread.sleep()`은 밀리초 단위로 늦게 깨어날 수 있어, 실제 처리량이
약 7,000 OP/s 수준으로 제한됐다. 이 값은 ZNS/파일시스템 성능이 아니라 Java
스레드 wake-up 정밀도의 영향을 크게 받은 결과다.

### 수정 내용

`ProducerTask`의 rate limiter를 절대 시각 기반 hybrid pacing으로 교체했다.

1. `nextSendNs`에 다음 요청의 예정 시각을 누적한다.
2. 예정 시각까지 200µs보다 많이 남았으면 `LockSupport.parkNanos()`로 대기한다.
3. 마지막 200µs 이하에서는 `Thread.onSpinWait()`로 대기한다.
4. 각 요청 뒤에는 실제 요청 완료 시각이 아니라 예정 시각 기준으로
   `nextSendNs += intervalNs`를 수행한다.

따라서 짧은 sleep의 wake-up 오차가 매 요청마다 누적되지 않는다. 1KB/100K OP/s
조건처럼 수십 µs 간격의 부하도 설정한 target OP/s에 가깝게 발생시킬 수 있다.

### 유의 사항

- 이 수정은 producer가 생성하는 부하의 정확도를 높이는 것이다. 이후에도 목표
  OP/s에 도달하지 못하거나 p99 latency가 증가하면, 그 값은 Kafka·DM·ZNS 경로의
  실제 포화/큐잉 현상으로 해석할 수 있다.
- spin 구간은 높은 target OP/s에서 CPU를 사용한다. 이는 짧은 간격을 정확히
  만들기 위한 의도된 trade-off이므로, 결과 분석 시 CPU 사용률도 함께 기록한다.
- 수정 후에는 `./gradlew clean jar`로 JAR를 다시 빌드해야 한다.

### Pacing 수정 전·후 결과 해석

동일한 target OP/s 설정으로 pacing 수정 전과 수정 후의 벤치마크 결과를
비교할 때는 단순한 성능 향상 또는 성능 저하로 해석하면 안 된다. 수정 전에는
부하 생성기 자체가 목표 속도에 도달하지 못했지만, 수정 후에는 실제 시스템의
처리 한계를 넘는 부하까지 생성할 수 있기 때문이다.

#### 수정 전 결과

- 1KB와 일부 10KB 조건에서는 Java 부하 생성기가 병목이었다.
- 특히 1KB 결과는 filesystem의 최대 성능을 측정한 것이 아니다.
- 1KB/100,000 OP/s/producer 8개 조건에서 producer별 요청 간격은 약 80µs지만,
  기존 `Thread.sleep()`은 이 간격을 정확히 지키지 못했다.
- 실제 처리량이 약 7,000 OP/s로 제한되고 disk utilization과 write throughput도
  낮았으므로, 저장장치가 충분히 사용되지 않은 상태였다.
- 당시의 낮은 app latency는 저장장치가 특별히 빨랐기 때문이 아니라 실제로
  생성된 요청이 적어 Kafka와 스토리지 queue가 거의 쌓이지 않았기 때문이다.
- 따라서 수정 전의 1KB 결과는 filesystem saturation 또는 최대 throughput 비교
  자료로 사용하기 어렵다.

#### 수정 후 결과

- 1KB 조건에서도 실제로 수십 MB/s의 부하가 발생한다.
- record size별 처리 bandwidth가 대략 30~60MB/s 범위로 수렴하면서, 수정 전
  1KB 결과에서 나타났던 약 5~7MB/s의 인위적인 제한이 사라졌다.
- Kafka, DM, ZNS 경로의 실제 포화와 queueing 현상이 관찰된다.
- 실제 저장 경로에 충분한 부하를 전달하므로 처리량 측정은 수정 전보다 의미가 있다.
- 목표 부하가 시스템의 실제 처리 능력보다 높으면 완료되지 못한 요청이 queue에
  누적되므로 app 평균 및 p99 latency가 수 초 이상으로 증가할 수 있다.
- 따라서 현재의 높은 target OP/s 결과는 최대 처리량 및 포화 특성 분석에는 사용할
  수 있지만, 정상 부하 상태의 filesystem latency 비교에는 적합하지 않다.
- `send_errors > 0`이거나 `total_requests == 0`인 Dynamic topic 결과는 정상적인
  성능 결과가 아니므로 비교 대상에서 제외하고 `INVALID`로 처리해야 한다.

#### 결과 해석 시 기준

- 수정 전과 수정 후의 1KB 처리량 차이는 filesystem 자체가 갑자기 빨라진 것이
  아니라, pacing 수정으로 실제 offered load가 증가한 결과다.
- 수정 후에도 achieved OP/s가 target에 미달하는 것은 pacing 실패를 바로 의미하지
  않는다. Kafka producer buffer, broker queue, DM 또는 ZNS의 처리 한계로 인해
  backpressure가 발생한 결과일 수 있다.
- 최대 처리량 실험에서는 `achieved_ops`, write throughput, disk utilization 및
  send error를 중심으로 해석한다.
- latency 비교 실험에서는 target을 관찰된 최대 처리량보다 충분히 낮게 설정하여
  지속적으로 queue가 증가하지 않도록 해야 한다.
- 결과의 변동성을 확인하려면 각 조건을 최소 3회 이상 반복하고, spin pacing으로
  증가할 수 있는 CPU 사용률도 함께 비교해야 한다.

## 측정 구간 정확성 수정

### 수정 이유

기존 코드는 `durationSec=60` 안에 warmup 20초를 포함했기 때문에 실제 latency
측정 구간은 약 40초였다. 그러나 Python 보고서에는 measurement 60초와 warmup
20초가 별도 구간인 것처럼 표시되어 실행 조건과 보고서가 일치하지 않았다.

또한 producer의 비동기 요청 생성을 중단한 뒤 `flush()`와 `close()`가 끝난
시점에서 전체 경과 시간을 구하고 warmup만 빼서 throughput 분모로 사용했다.
포화 상태에서 drain/flush가 오래 걸릴수록 분모가 증가하여 동일한 measurement
시간에도 `Achieved OP/s`가 달라질 수 있었다.

마지막으로 latency 포함 여부를 send 시각이 아니라 callback의 ACK 시각으로
판정했다. 그 결과 warmup 중 전송한 요청이 measurement 중 ACK되면 결과에 섞이고,
measurement 중 전송한 요청과 drain 구간의 관계도 명확하지 않았다.

### 수정 내용

실행 시간을 다음 세 구간으로 명확하게 분리했다.

```text
Warmup 20초
→ Measurement 60초
→ Drain/flush 최대 180초
```

CLI 인자를 다음과 같이 분리했다.

```text
--warmup-sec 20
--measure-sec 60
--drain-timeout-sec 180
```

기존 자동화 코드와의 호환성을 위해 `--duration`은 `--measure-sec`의 alias로
남겨 두었지만, 새 Python 실행기는 `--measure-sec`를 사용한다.

Java는 `System.nanoTime()`을 이용해 monotonic clock 기준 measurement 경계를
계산한다.

```java
long startTimeNs = System.nanoTime();
measureStartNs = startTimeNs + TimeUnit.SECONDS.toNanos(warmupSec);
measureEndNs = measureStartNs + TimeUnit.SECONDS.toNanos(measureSec);
```

Producer는 warmup과 measurement를 합친 구간에만 요청을 생성하며,
`measureEndNs` 이후에는 새로운 요청을 만들지 않는다. 이후 `flush()`는 이미
전송된 요청의 ACK와 callback을 마무리하는 drain 단계로만 사용한다.

각 요청이 measurement 결과에 포함되는지는 callback 시각이 아니라 send 시각에
결정한다.

```java
final long sendStart = System.nanoTime();
final boolean measured = sendStart >= measureStartNs
        && sendStart < measureEndNs;
```

따라서 다음 규칙이 적용된다.

- warmup 중 send되고 measurement 중 ACK된 요청은 결과에서 제외한다.
- measurement 중 send되고 drain 중 ACK된 요청은 전체 latency와 함께 포함한다.
- throughput 분모는 drain 시간과 관계없이 항상 `measureSec`로 고정한다.
- 실제 drain 소요 시간과 완료 여부는 `Drain Time`, `Drain Completed`로 별도 출력한다.

Python 실행기도 `MEASURE_DURATION`과 `DRAIN_TIMEOUT_SECONDS`를 각각 Java의
`--measure-sec`, `--drain-timeout-sec`로 전달하도록 수정했다. CSV에는
`measure_sec`, `drain_timeout_sec`, `drain_time_sec`, `drain_completed`를 저장한다.
`iostat`과 `vmstat` 파싱도 warmup 이후 최대 `measure_sec`개의 샘플만 사용하여
drain/flush 구간의 시스템 지표가 measurement 평균에 포함되지 않도록 했다.

### 해석 시 유의 사항

현재 `Achieved OP/s`는 measurement 구간에 send된 뒤 drain까지 기다려 성공적으로
ACK된 요청 수를 고정된 `measureSec`로 나눈 값이다. `Drain Time`이 길다면 측정 종료
시점에 backlog가 많이 남아 있었다는 의미이므로, 처리량과 함께 확인해야 한다.
`Drain Completed=false`인 run은 callback과 latency 표본이 완전하지 않을 수 있어
정상 비교 결과에서 제외해야 한다.
