# #!/bin/bash

# JARS="/opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar"

# JOBNAME="DroneDataAnalysis"
# # SCRIPT="/opt/airflow/jobs/main.py"
# SCRIPT="$1"
# echo ${SCRIPT}
# # docker exec -it de-2024_spark-master_1 spark-submit \
# spark-submit \
#   --name ${JOBNAME} \
#   --master spark://spark-master:7077 \
#   --jars ${JARS} \
#   --conf spark.dynamicAllocation.enabled=true \
#   --conf spark.dynamicAllocation.executorIdleTimeout=2m \
#   --conf spark.dynamicAllocation.minExecutors=1 \
#   --conf spark.dynamicAllocation.maxExecutors=3 \
#   --conf spark.dynamicAllocation.initialExecutors=1 \
#   --conf spark.memory.offHeap.enabled=true \
#   --conf spark.memory.offHeap.size=2G \
#   --conf spark.shuffle.service.enabled=true \
#   --conf spark.executor.memory=3G \
#   --conf spark.driver.memory=3G \
#   --conf spark.driver.maxResultSize=0 \
#   --num-executors 2 \
#   --executor-cores 1 \
#   ${SCRIPT}
# #docker exec -it de-2024_spark-master_1 sh spark-submit.sh


#!/bin/bash

JARS="/opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar"

JOBNAME="DroneDataAnalysis"
SCRIPT="$1"
echo "Running script: ${SCRIPT}"

docker exec -i de-2024_spark-master_1 /opt/bitnami/spark/bin/spark-submit \
  --name ${JOBNAME} \
  --master spark://spark-master:7077 \
  --jars ${JARS} \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.executorIdleTimeout=2m \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=3 \
  --conf spark.dynamicAllocation.initialExecutors=1 \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=2G \
  --conf spark.shuffle.service.enabled=true \
  --conf spark.executor.memory=3G \
  --conf spark.driver.memory=3G \
  --conf spark.driver.maxResultSize=0 \
  --num-executors 2 \
  --executor-cores 1 \
  /opt/bitnami/spark/jobs/main.py  # Spark 마스터 컨테이너에서 접근 가능한 경로

