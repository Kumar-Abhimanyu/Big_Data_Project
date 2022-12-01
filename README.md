# BD1_009_055_197_224

Project Title : Yet Another Kafka

To run:

**first run brokers**:<br>
python broker1.py<br>
python broker2.py<br>
python broker3.py<br>

**then run zookeeper**:<br>
python zookeeper.py<br>

**run consumer and producer on ports of your choice (except 5000,5001,5002)**:<br>
python producer.py "port"<br>
python consumer.py "port"<br>
<br><br>
(can run multiple producers and consumers on different ports if necessary)<br><br>

In order to perform producer or consumer operations like publish, or subscribe, go to localhost:[port]<br><br>

NOTE: before exiting consumer and producer, remember to deregister producer and unsubscribe the consumer
