set -e
echo "Waiting for brokers..."
sleep 8
rpk cluster info --brokers lab-rp1:9092 || true
rpk topic create lab.events -p 10 -r 3 --brokers lab-rp1:9092,lab-rp2:9092,lab-rp3:9092 || true
rpk topic describe lab.events --brokers lab-rp1:9092