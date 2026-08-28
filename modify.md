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

## 2026-08-29 — 현재 해결해야 할 문제

최근 결과에서는 pacing과 measurement 구간 분리 수정이 정상적으로 적용됐다.
유효한 run에서 `total_requests / 60초`와 `Achieved OP/s`가 일치하므로 기존의
drain 시간 포함 문제는 해결됐다. 다만 아래 문제들이 남아 있어, 현재 결과만으로
filesystem의 정상 부하 latency나 안정적인 우열을 결론 내리기는 어렵다.

### 1순위: 포화 실험과 latency 실험 분리

현재 record size별 target은 모두 약 100MB/s의 offered load를 만든다.

```text
1KB    × 100,000 OP/s ≈ 100MB/s
10KB   ×  10,000 OP/s ≈ 100MB/s
100KB  ×   1,000 OP/s ≈ 100MB/s
1MB    ×     100 OP/s ≈ 100MB/s
```

반면 실제 Kafka·DM·ZNS 경로의 처리량은 대체로 약 30~60MB/s 범위이므로 요청이
처리 속도보다 빠르게 쌓인다. 그 결과 app p99가 수십 초까지 증가하고 drain도
길어진다. 이는 측정기 오류라기보다 지속적인 overload와 queueing을 관찰한 값이다.

따라서 실험을 두 종류로 분리해야 한다.

- Saturation profile: 현재처럼 높은 target으로 최대 처리량과 포화 특성을 측정한다.
- Latency profile: 최대 처리량보다 충분히 낮은 target에서 filesystem latency를
  비교한다. 초기 후보는 약 20MB/s인 `{1KB: 20000, 10KB: 2000,
  100KB: 200, 1MB: 20}`이다.
- Latency profile에서는 drain이 짧고 queue가 지속적으로 증가하지 않는지 확인한
  뒤 latency를 비교해야 한다.

### 2순위: 처리량 지표의 의미 분리

현재 `Achieved OP/s`는 measurement 구간에 send된 요청 중 drain 종료 전까지
ACK된 성공 요청 수를 `measureSec`로 나눈 값이다. 따라서 measurement 종료 시점에
처리되지 않았던 요청도 drain 중 ACK되면 achieved 수에 들어간다. 분모는 정확해졌지만
이 값만으로 measurement 60초 동안 broker가 실제 완료한 지속 가능 처리량을 알 수는
없다.

다음 지표를 별도로 출력해야 한다.

- `Sent OP/s`: measurement 구간에 생성한 요청 수 / measurement 시간
- `ACK Window OP/s`: measurement 구간 안에서 ACK까지 끝난 요청 수 / measurement 시간
- `Eventual ACK OP/s`: measurement 중 send되고 drain까지 ACK된 요청 수 / measurement 시간
- `Outstanding at End`: measurement 종료 시점의 미완료 요청 수
- `Failed/Timed-out Requests`: 실패 및 drain timeout으로 미완료된 요청 수

이렇게 분리해야 offered load, 실제 측정 구간 완료량, drain으로 넘어간 backlog를
혼동하지 않는다.

### 3순위: 무제한 outstanding 요청과 latency 폭증 방지

비동기 producer가 backend 처리 속도보다 빠르게 요청을 계속 생성하면 Kafka producer
buffer와 내부 queue에 매우 많은 요청이 쌓인다. 이때 app latency는 filesystem 처리
시간뿐 아니라 producer buffer 대기, Kafka queueing, broker 처리 및 ACK 대기를 모두
포함하므로 수 초에서 수십 초까지 증가할 수 있다.

정상 부하 latency 실험에는 semaphore 등으로 최대 in-flight 요청 수를 제한하는
옵션이 필요하다. 제한에 걸린 시간 또는 횟수도 backpressure 지표로 기록해야 한다.
다만 saturation 실험에서는 이 제한이 offered load를 바꾸므로 on/off 가능한 옵션으로
두고, 설정값을 보고서에 반드시 남겨야 한다.

### 4순위: pacing의 catch-up burst 제한

현재 absolute-time pacing은 누적 wake-up 오차를 막는 데 유효하지만, producer가
예정 시각보다 크게 늦어진 경우 밀린 schedule을 따라잡으려고 요청을 연속 전송할 수
있다. 이 catch-up burst가 순간적인 queue 증가와 tail latency를 악화시킬 수 있다.

최대 catch-up 요청 수 또는 최대 허용 지연을 정하고, 임계값을 넘으면 `nextSendNs`를
현재 시각 기준으로 재설정하는 옵션을 추가해야 한다. 발생 횟수도 결과에 기록하여
목표 rate 유지와 burst 억제의 trade-off를 확인해야 한다.

### 5순위: Dynamic topic 오류 원인 분리 및 무효 처리

최근 Dynamic 결과에는 `send_errors > 0`, `total_requests == 0`, 80~100초 이상의
긴 drain이 반복된다. 특히 실행 시간이 기존 60초에서 warmup 20초 + measurement
60초로 늘면서 topic 생성기도 더 오래 동작하여, 5 topics/s 기준 최대 약 400개의
topic이 만들어질 수 있다. topic 생성과 metadata 갱신 부하가 filesystem 비교를
압도할 가능성이 있다.

- `send_errors > 0`, `total_requests == 0`, `Drain Completed=false`인 run은 자동으로
  `INVALID` 처리한다.
- 최초 오류 유형, 예외 메시지, 오류가 시작된 시각 및 topic 수를 결과에 저장한다.
- topic 생성 성공/실패 수와 최종 topic 수를 별도 지표로 출력한다.
- filesystem baseline과 Dynamic topic 실험을 별도 실행 모드로 분리한다.
- Dynamic rate 5/s가 목적에 맞는지 검토하고 낮은 rate부터 단계적으로 올린다.

### 6순위: 모니터링 대상과 bottleneck 판정 개선

현재 `iostat await` 하나만으로는 `/dev/mapper/kafka-zns`, raw ZNS 장치, DM 내부에서
어디에 지연이 생겼는지 구분하기 어렵다. mapper와 raw device를 동시에 기록하고,
가능하면 DM 통계와 Kafka producer/broker 지표도 함께 수집해야 한다.

또한 현재 bottleneck 판정은 높은 await나 iowait가 있어도 util 조건을 동시에
만족하지 않으면 `false`가 될 수 있다. Dynamic 오류로 부하가 중간에 끊긴 run도
낮은 평균 util 때문에 정상처럼 보일 수 있으므로, 단일 boolean 대신 다음 상태를
구분하는 편이 안전하다.

- `SATURATED`: 높은 utilization과 지속적인 backlog가 확인됨
- `BACKPRESSURED`: 긴 drain 또는 outstanding 증가가 확인됨
- `FAILED`: send error, zero success 또는 drain timeout 발생
- `NOT_SATURATED`: 낮은 부하에서 오류와 backlog가 없음
