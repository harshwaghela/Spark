from pyspark import SparkContext,SparkConf
import json
import time
import sys

conf = SparkConf().setAppName('Task2').setMaster("local[*]")
sc= SparkContext(conf=conf)
input_file=sys.argv[1]
output_file=sys.argv[2]
num_partitions=int(sys.argv[3])

output= {"default":
             {"n_partition":None,
              "n_items":None,
              "exe_time":None
              },
         "customized":
             {
                 "n_partition":None,
                 "n_items":None,
                 "exe_time":None
             },
         "explanation":"The number of partitions impact the performance of the program depending upon the number of cores available in the cluster. Starting with the number of partitions as 8 I saw execution time decrease upon increasing the number of partitions until a certain value after which time increases again. At this point management of the cluster partitions becomes more expensive and we do not gain any benefit from increasing them any more. "
         }

input_rdd = sc.textFile(input_file)
rdd =input_rdd.map(json.loads)
start_default=time.time()
new_rdd= rdd.repartition(8)
output['default']['n_partition']=new_rdd.getNumPartitions()
output['default']['n_items']=new_rdd.mapPartitions(lambda x: [sum(1 for i in x)]).collect()
top10_business= new_rdd.map(lambda x: (x['business_id'],1)).reduceByKey(lambda x,y:x+y).takeOrdered(10,lambda x: -x[1])
end_default=time.time()
default_time= end_default-start_default
output['default']['exe_time']=default_time

start_custom=time.time()
new_rdd= rdd.repartition(num_partitions)
output['customized']['n_partition']=new_rdd.getNumPartitions()
output['customized']['n_items']=new_rdd.mapPartitions(lambda x: [sum(1 for i in x)]).collect()
top10_business= new_rdd.map(lambda x: (x['business_id'],1)).reduceByKey(lambda x,y:x+y).takeOrdered(10,lambda x: -x[1])
end_custom=time.time()
custom_time= end_custom-start_custom
output['customized']['exe_time']=custom_time
with open(output_file,'w') as outfile:
    outfile.write(json.dumps(output))
