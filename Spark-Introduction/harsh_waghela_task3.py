import json
from pyspark import SparkContext,SparkConf
import time
import sys

input1= sys.argv[1]
input2= sys.argv[2]
output_file_1=sys.argv[3]
output_file_2=sys.argv[4]
prog_start=time.time()
output_taskb={"m1": None,"m2": None, "explanation":""
              }
conf = SparkConf().setAppName('Task3').setMaster("local[*]")
sc = SparkContext()
rdd1 = sc.textFile(input1)
rdd2 = sc.textFile(input2)
rdd1 =rdd1.map(json.loads).map(lambda x:(x['business_id'],x['stars']))
rdd2 = rdd2.map(json.loads).map(lambda x:(x['business_id'],x['city']))

joined = rdd2.join(rdd1).map(lambda x: (x[1][0],(x[1][1],1)))\
    .reduceByKey(lambda x,y : (x[0]+y[0],x[1]+y[1]))\
    .map(lambda x:(x[0],x[1][0]/x[1][1])).sortByKey().sortBy(lambda x:x[1],ascending=False)

start_time=time.time()
collect_output= joined.collect()
for i in range(10):
    print(collect_output[i])
end_time= time.time() - start_time
output_taskb["m1"]=end_time

output='city,stars\n'
for i in range(len(collect_output)):
    output+= str(collect_output[i][0])+','+str(collect_output[i][1])+'\n'

with open(output_file_1,'w') as fout:
    fout.write(output)


start_time_take=time.time()
take_output=joined.take(10)
for i in take_output:
    print(i)
end_time_take=time.time()-start_time_take
output_taskb["m2"]=end_time_take
output_taskb["explanation"]= 'The time taken by the take action\
            while running locally on my system was 0.09 sec compared\
            to 1.31 sec when using the collect action. This is mainly due to the lazy nature of take\
            that only necessary values get calculated during the previous sort operation whereas all the data is returned\
             for collect action causing it to take longer time.'




with open(output_file_2,'w') as fout2:
    fout2.write(json.dumps(output_taskb))

print("Time is :"+str(time.time()-prog_start))



