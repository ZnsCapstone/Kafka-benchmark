# KafkaBenchmark 수정 기록

## 2026-08-28 — 고빈도 OP/s pacing 개선

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
