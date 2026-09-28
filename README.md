# 🚨 Kafka Streams — Real-Time Fraud Detection

A Kafka Streams example that reads banking transactions in real time, flags any transaction over a set threshold, and writes the flagged ones to a separate topic.

---

## 🏗 Flow

```
Producer → transactions topic → Kafka Streams (filter > 10,000) → fraud-alerts topic
```

1. The producer publishes transaction events to the `transactions` topic.
2. The Kafka Streams app reads each event and filters out any transaction over `10,000`.
3. Flagged transactions are written to the `fraud-alerts` topic.

---

## 🛠 Tech Stack

| Category | Technology |
|---|---|
| Language / Framework | Java 21, Spring Boot |
| Streaming | Spring for Apache Kafka Streams |
| Messaging | Apache Kafka |
| API Testing | Swagger UI |

---

## 📋 Prerequisites

- [ ] Java 21
- [ ] Apache Kafka installed locally
- [ ] Maven Wrapper

---

## 🚀 Running the Project

### 1. Start Kafka (KRaft Mode)

```bash
# Generate a Cluster ID
bin/kafka-storage.sh random-uuid

# Format the log directories (replace <CLUSTER_ID> with the value generated above)
bin/windows/kafka-storage.bat format -t <CLUSTER_ID> -c config/server.properties

# Start the broker
bin/windows/kafka-server-start.bat config/server.properties
```

**2. Run the application**
```
./mvnw spring-boot:run
```
Runs on `http://localhost:9191`. Watch the console — the stream state should move from `REBALANCING` to `RUNNING`.

**3. Publish transactions**
Open Swagger at `http://localhost:9191/swagger-ui/index.html`, find the `transactions` endpoint, and click **Try it out → Execute**. No request body needed — each call publishes a batch of random transactions.

**4. Verify the results**
- Console log: filter for `Fraud alert` to see flagged transactions.
- Offset Explorer: `transactions` should hold all published events; `fraud-alerts` should hold only the ones over `10,000`.

---

## 📝 Notes

- The `10,000` threshold is hardcoded for this demo.
- Kafka Streams persists its internal state using RocksDB, which is why the app briefly shows `REBALANCING` before reaching `RUNNING`.
