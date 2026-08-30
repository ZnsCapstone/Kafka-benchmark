# KafkaBenchmark 수정 기록

## 2026-08-30 — 측정 지표·backpressure·실행 안정성 개선

서버 재실험 전에 Java 부하기가 offered load, 측정 구간 완료량, drain 완료량과
backlog를 서로 다른 값으로 출력하도록 수정했다. 기존 Python 파서 호환을 위해
`Total Requests`, `Achieved OP/s`, `Total Sent`, `Send Errors`도 유지한다.
`Achieved OP/s`는 이제 measurement 중 send되어 drain까지 성공적으로 ACK된
`Eventual ACK OP/s`와 같은 의미다.

추가된 지표는 다음과 같다.

```text
Sent Requests / Sent OP/s
ACK Window Requests / ACK Window OP/s
Eventual ACK Requests / Eventual ACK OP/s
Outstanding at End
Failed Requests
Unresolved After Drain
Latency Dropped Samples
Backpressure Wait Count / Time
Max Observed Outstanding
Catch-up Resets / Records Skipped / Max Schedule Lag
```

### 지표 정의와 해석

| 지표 | 정의 | 해석 시 주의점 |
|---|---|---|
| `Target OP/s` | 사용자가 지정한 전체 producer 목표 전송률 | 실제 생성량이 아니며 `Sent OP/s`와 비교해야 한다. |
| `Sent Requests` | measurement 구간에 `send()`를 시도한 요청 수 | 동기 send 실패도 offered load와 실패 요청으로 센다. |
| `Sent OP/s` | `Sent Requests / measureSec` | 부하기가 실제로 생성한 offered load다. |
| `ACK Window Requests` | measurement 중 send되고 measurement 종료 전에 성공 ACK된 수 | 측정 창 안에서 완료된 양이며 지속 가능한 처리량 판단에 가장 가깝다. |
| `ACK Window OP/s` | `ACK Window Requests / measureSec` | `Sent OP/s`보다 계속 낮으면 backlog가 증가하고 있다는 뜻이다. |
| `Eventual ACK Requests` | measurement 중 send되고 drain 종료 전 성공 ACK된 수 | measurement 이후 처리된 backlog도 포함한다. |
| `Eventual ACK OP/s` | `Eventual ACK Requests / measureSec` | drain 처리량이 아니라 eventual completion 비율이다. 기존 `Achieved OP/s`와 같다. |
| `Outstanding at End` | measurement 종료 시 callback이 끝나지 않은 measurement 요청 수 | 값이 크면 latency 표본에 지속적인 queueing이 포함됐다는 뜻이다. |
| `Failed Requests` | measurement 요청 중 동기 send 또는 비동기 callback이 실패한 수 | 0보다 크면 정상 성능 비교에서 제외한다. |
| `Unresolved After Drain` | sent에서 성공 ACK와 확인된 실패를 뺀 drain 종료 후 미해결 수 | 0보다 크면 결과가 불완전하다. |
| `Latency Dropped Samples` | 성공 ACK됐지만 latency 배열 용량 때문에 저장하지 못한 수 | 0보다 크면 percentile이 전체 요청을 대표하지 않으므로 INVALID다. |
| `Drain Time` | measurement 종료 후 producer 종료까지 걸린 시간 | 처리량 분모에는 포함되지 않는다. 길면 종료 시 backlog가 컸다는 신호다. |
| `Drain Completed` | 모든 producer가 timeout 안에 flush와 close를 마쳤는지 여부 | false면 callback과 latency 통계가 완전하지 않을 수 있다. |
| `Backpressure Wait Count` | outstanding semaphore permit을 즉시 얻지 못한 횟수 | limiter가 실제 offered load를 억제한 빈도를 나타낸다. |
| `Backpressure Wait Time` | permit을 기다린 누적 시간 | producer 전체 합계이므로 wall-clock 시간보다 클 수 있다. |
| `Max Observed Outstanding` | warmup과 measurement 동안 callback 대기 중이던 요청의 최대 개수 | limiter 사용 시 설정값을 넘지 않아야 한다. |
| `Catch-up Resets` | pacing 지연이 제한을 넘어 schedule을 현재 시각으로 재설정한 횟수 | 많으면 target 유지보다 burst 억제가 자주 적용된 것이다. |
| `Catch-up Records Skipped` | schedule reset으로 따라잡지 않은 예정 요청 수의 추정치 | 실제 실패 요청이 아니라 의도적으로 생성하지 않은 부하다. |
| `Max Schedule Lag` | producer가 예정 send 시각보다 가장 많이 늦어진 시간 | CPU scheduling, backpressure와 `send()` block의 영향을 모두 받을 수 있다. |
| `Total Requests` | 저장된 measurement latency 표본 수 | 기존 파서 호환 지표다. 정상 run에서는 Eventual ACK Requests와 같다. |
| `Total Sent (incl. warmup)` | warmup과 measurement 요청 중 성공 ACK된 총수 | 이름과 달리 send 시도 수가 아니라 성공 callback 수이므로 신규 분석에는 사용하지 않는다. |
| `Send Errors` | warmup과 measurement 전체의 동기·비동기 send 오류 수 | `Failed Requests`보다 범위가 넓은 호환 지표다. |

latency percentile은 measurement 중 send되고 drain 종료 전 성공 ACK되어 표본에
저장된 요청의 `send()` 진입부터 callback까지 시간이다. producer buffer 대기,
network, broker 처리와 ACK가 포함되며 디스크 영구 기록 latency는 아니다.

실행 안정성도 함께 개선했다.

- 600초 × 초당 100,000개의 전역 latency 배열을 제거했다. 전체 latency 표본에
  ACK 초를 함께 저장해 초 단위 percentile을 계산하므로 약 458MiB의 고정 할당과
  600초 배열 제한이 사라졌다.
- 초 구간 계산을 `currentTimeMillis()`에서 monotonic `nanoTime()`으로 변경했다.
- target OP/s의 나머지를 producer에 분배하고, target보다 producer가 많으면 일부
  producer에 0 OP/s를 부여하여 전체 합계가 항상 target과 같게 했다.
- producer 초기화 timeout과 초기화 실패 전파를 추가해 `readyLatch` 무한 대기를
  방지했다.
- CLI의 누락 값, 알 수 없는 옵션, 잘못된 boolean 및 음수 값을 오류 처리한다.
- `--bootstrap-servers`, `--topic`, `--producer-start-timeout-sec`를 추가했다.
- dynamic topic 이름에 실행별 UUID prefix를 넣어 이전 실행 토픽과 충돌하지 않게
  했으며 최초 실패 유형과 발생 시간을 출력한다.

정상 부하 latency 실험을 위한 선택형 outstanding 제한을 추가했다.

```text
--max-in-flight-records 0      # 기본값: 제한 없음, saturation profile
--max-in-flight-records 1000   # 예시: 전체 producer 합산 최대 1000 records
```

제한은 Java 애플리케이션이 callback을 기다리는 전체 record 수에 적용된다.
Kafka의 `max.in.flight.requests.per.connection=5`와는 다른 계층이다.

catch-up burst 억제 옵션도 추가했다. 둘 중 먼저 도달하는 제한을 사용하며 0은
해당 제한을 사용하지 않는다는 뜻이다.

```text
--max-catch-up-records 10
--max-schedule-lag-ms 100
```

임계값을 넘으면 `nextSendNs`를 현재 시각으로 재설정한다. 이때 reset 횟수,
건너뛴 catch-up record 수와 최대 schedule lag를 결과에 기록한다.

단위 테스트는 target 분배와 latency capacity 경계를 검증한다. 로컬에서
`./gradlew test jar`가 통과했으며, 실제 Kafka/ZNS/FEMU 동작은 서버에서 검증해야
한다.

### Python 실행기에 남은 연동 작업

새 지표를 CSV/JSON에 저장하고 다음 조건을 자동 `INVALID` 처리해야 한다.

```text
Java exit code != 0
Eventual ACK Requests == 0
Failed Requests > 0
Drain Completed == false
Unresolved After Drain > 0
Latency Dropped Samples > 0
```

`INVALID` run은 원본과 CSV에는 남기되 집계 평균에서는 제외한다. 이후 saturation과
latency profile을 분리하고 mapper 및 raw ZNS 장치를 동시에 모니터링한다.

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

## 2026-08-30 — Consumer 정합성 및 ACK stall 측정

- 모든 producer record에 고정 producer key와 producer ID/sequence header를 넣는다.
  같은 producer의 레코드는 같은 partition으로 전송되므로 consumer가 producer별
  순서, gap, duplicate를 검사할 수 있다.
- payload는 기존과 동일한 고정 크기를 유지하고 consumer가 길이와 CRC32를 확인한다.
- producer flush가 끝난 뒤 consumer가 연속 10초 동안 새 레코드가 없을 때까지
  drain한다. producer 성공 ACK 수와 최종 consume 수가 다르면 정합성 실패다.
- malformed header, payload CRC 오류, sequence gap, duplicate, out-of-order를 각각
  출력한다. 이 지표는 성능 latency와 별개이며 하나라도 발생하면 run을 무효화한다.
- measurement 구간에서 ACK가 0인 연속 초를 stall count/total/max로 출력한다.
  평균 disk util이 낮아도 긴 zero-ACK 정지를 별도로 식별할 수 있다.

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

## 2026-08-29 — 문제 목록 및 현재 상태

이 절은 최초 문제 보고를 보존하면서 2026-08-30 기준 진행 상태를 표시한다.
Java 부하기에서 해결된 항목과 Python 실행기·모니터링에 남은 항목을 구분한다.

```text
1. 포화/latency profile 분리        미완료 (Python)
2. 처리량 지표 의미 분리             완료   (Java)
3. 최대 outstanding 제한            완료   (Java)
4. catch-up burst 제한               완료   (Java)
5. Dynamic topic 오류 처리           부분 완료
6. 모니터링 및 상태 판정              미완료 (Python)
```

최근 결과에서는 pacing과 measurement 구간 분리 수정이 정상적으로 적용됐다.
유효한 run에서 `total_requests / 60초`와 `Achieved OP/s`가 일치하므로 기존의
drain 시간 포함 문제는 해결됐다. 다만 아래 문제들이 남아 있어, 현재 결과만으로
filesystem의 정상 부하 latency나 안정적인 우열을 결론 내리기는 어렵다.

### 1순위: 포화 실험과 latency 실험 분리 — 미완료

Java 옵션은 준비됐지만 Python 실행기의 profile 구성과 결과 구분은 아직 필요하다.

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

### 2순위: 처리량 지표의 의미 분리 — 완료

2026-08-30 Java 수정에서 아래 지표를 모두 분리했다. 기존 `Achieved OP/s`는
호환을 위해 유지하며 현재는 `Eventual ACK OP/s`와 같은 값이다.

현재 `Achieved OP/s`는 measurement 구간에 send된 요청 중 drain 종료 전까지
ACK된 성공 요청 수를 `measureSec`로 나눈 값이다. 따라서 measurement 종료 시점에
처리되지 않았던 요청도 drain 중 ACK되면 achieved 수에 들어간다. 분모는 정확해졌지만
이 값만으로 measurement 60초 동안 broker가 실제 완료한 지속 가능 처리량을 알 수는
없다.

다음 지표를 별도로 출력하도록 수정했다.

- `Sent OP/s`: measurement 구간에 생성한 요청 수 / measurement 시간
- `ACK Window OP/s`: measurement 구간 안에서 ACK까지 끝난 요청 수 / measurement 시간
- `Eventual ACK OP/s`: measurement 중 send되고 drain까지 ACK된 요청 수 / measurement 시간
- `Outstanding at End`: measurement 종료 시점의 미완료 요청 수
- `Failed/Timed-out Requests`: 실패 및 drain timeout으로 미완료된 요청 수

이렇게 분리해야 offered load, 실제 측정 구간 완료량, drain으로 넘어간 backlog를
혼동하지 않는다.

### 3순위: 무제한 outstanding 요청과 latency 폭증 방지 — 완료

`--max-in-flight-records`와 backpressure 대기 횟수·시간 및 최대 outstanding
지표를 추가했다. 기본값 0은 saturation 실험을 위한 무제한 모드다.

비동기 producer가 backend 처리 속도보다 빠르게 요청을 계속 생성하면 Kafka producer
buffer와 내부 queue에 매우 많은 요청이 쌓인다. 이때 app latency는 filesystem 처리
시간뿐 아니라 producer buffer 대기, Kafka queueing, broker 처리 및 ACK 대기를 모두
포함하므로 수 초에서 수십 초까지 증가할 수 있다.

정상 부하 latency 실험에는 semaphore 기반 최대 in-flight 요청 제한을 사용한다.
제한에 걸린 시간과 횟수도 backpressure 지표로 기록한다.
다만 saturation 실험에서는 이 제한이 offered load를 바꾸므로 on/off 가능한 옵션으로
두고, 설정값을 보고서에 반드시 남겨야 한다.

### 4순위: pacing의 catch-up burst 제한 — 완료

`--max-catch-up-records`, `--max-schedule-lag-ms`와 reset·skip·최대 lag 지표를
추가했다. 두 옵션의 기본값 0은 기존 absolute pacing 동작을 유지한다.

현재 absolute-time pacing은 누적 wake-up 오차를 막는 데 유효하지만, producer가
예정 시각보다 크게 늦어진 경우 밀린 schedule을 따라잡으려고 요청을 연속 전송할 수
있다. 이 catch-up burst가 순간적인 queue 증가와 tail latency를 악화시킬 수 있다.

최대 catch-up 요청 수 또는 최대 허용 지연을 정하고, 임계값을 넘으면 `nextSendNs`를
현재 시각 기준으로 재설정한다. 발생 횟수도 결과에 기록하여
목표 rate 유지와 burst 억제의 trade-off를 확인해야 한다.

### 5순위: Dynamic topic 오류 원인 분리 및 무효 처리 — 부분 완료

Java는 실행별 UUID topic 이름, 생성 성공/실패 수, 최초 실패 유형과 발생 시간을
출력한다. Python의 자동 INVALID 판정, 최종 topic 수 확인, baseline과 별도 실행
모드 및 rate 단계화는 아직 필요하다.

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

### 6순위: 모니터링 대상과 bottleneck 판정 개선 — 미완료

이 항목은 Python 실행기에서 mapper/raw device 동시 수집과 상태 기반 판정을
구현해야 한다.

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
