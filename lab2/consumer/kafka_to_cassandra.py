import os, time, ujson as json
from kafka import KafkaConsumer
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.query import PreparedStatement

BROKERS = os.getenv("KAFKA_BROKERS","lab-rp1:9092").split(",")
TOPIC   = os.getenv("KAFKA_TOPIC","lab.events")
CASS    = os.getenv("CASSANDRA_HOST","cassandra")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BROKERS,
    group_id="lab-consumer-group",
    enable_auto_commit=True,
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# --- retry connect ---
for attempt in range(30):
    try:
        cluster = Cluster([CASS])
        session = cluster.connect()
        session.set_keyspace("labks")
        break
    except NoHostAvailable as e:
        print(f"[consumer] Cassandra not ready (attempt {attempt+1}/30): {e}")
        time.sleep(5)
else:
    raise SystemExit("[consumer] Cassandra never became available")

ps: PreparedStatement = session.prepare(
    "INSERT INTO users_by_country (country,id,name,email,created_at) VALUES (?,?,?,?,toTimestamp(now()))"
)

print("[consumer] started")
for msg in consumer:
    rec = msg.value
    session.execute(ps, (rec["country"], rec["id"], rec["name"], rec["email"]))
