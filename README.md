# BD1_009_055_197_224

Project Title : Yet Another Kafka

To run:

first run brokers:
python broker1.py
python broker2.py
python broker3.py

then run zookeeper:
python zookeeper.py

run consumer and producer on ports of your choice (except 5000,5001,5002):
python producer.py "port"
python consumer.py "port"

(can run multiple producers and consumers on different ports if necessary)

NOTE: before exiting consumer and producer, remember to deregister producer and unsubscribe the consumer
