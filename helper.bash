
# create topic
/usr/hdp/3.1.4.0-315/kafka/bin/kafka-topics.sh --create --topic lesson3_student782_7_topic --zookeeper bigdataanalytics2-worker-shdpt-v31-1-4:2181 --partitions 3 --replication-factor 2 --config retention.ms=17280000000

# update topic partitions, configuration, settings
/usr/hdp/3.1.4.0-315/kafka/bin/kafka-topics.sh --zookeeper bigdataanalytics2-worker-shdpt-v31-1-4:2181 --alter --config retention.ms=17280000000 --topic lesson2_student782_7_topic

# update topic partitions, configuration, settings
/usr/hdp/3.1.4.0-315/kafka/bin/kafka-topics.sh --zookeeper bigdataanalytics2-worker-shdpt-v31-1-4:2181 --alter --config retention.ms=17280000000 --topic lesson2_student782_7_topic 

# delete topic
/usr/hdp/3.1.4.0-315/kafka/bin/kafka-topics.sh --zookeeper bigdataanalytics2-worker-shdpt-v31-1-4:2181 --delete --topic lesson2_student782_7_topic 

# start console consumer
/usr/hdp/3.1.4.0-315/kafka/bin/kafka-console-consumer.sh --topic lesson2_student782_7_topic --from-beginning --bootstrap-server bigdataanalytics2-worker-shdpt-v31-1-4:6667 

# start console producer
/usr/hdp/3.1.4.0-315/kafka/bin/kafka-console-producer.sh --topic lesson2_student782_7_topic --broker-list bigdataanalytics2-worker-shdpt-v31-1-5:6667

# read topic’s partitions offsets
/usr/hdp/3.1.4.0-315/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list bigdataanalytics2-worker-shdpt-v31-1-5:6667 --topic lesson2_student782_7_topic 

# messages count in a topic
/usr/hdp/3.1.4.0-315/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list bigdataanalytics2-worker-shdpt-v31-1-5:6667 --topic lesson2_student782_7_topic --offsets 1 | awk -F ":" '{sum += $3} END {print sum}'
