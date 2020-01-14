from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import sys
import random
import os

port_number = int(sys.argv[1])
output_file_path = sys.argv[2]
sc = SparkContext()
ssc = StreamingContext(sc, 10)

random.seed(553)

def resovoier( status):
    global user_count,dam

    for user in status.collect():

            if user_count < 101:
                    dam.append(user)
            else:
                    if random.randint(0,100000) % user_count < 100:
                        random_index = random.randint(0, 100000) % 100
                        dam[random_index] = user
            if (user_count%100 ==0):
                fout.write(str(user_count)+","+str(dam[0])+','+str(dam[20])+','+str(dam[40])+','+str(dam[60])+','+str(dam[80])+'\n')
                fout.flush()
                os.fsync(fout.fileno())
            user_count += 1
    return

user_count = 1
dam = []
with open(output_file_path,'w') as fout:
    fout.write("seqnum,0_id,20_id,40_id,60_id,80_id\n")
fout=open('out3.txt','a+')
users = ssc.socketTextStream("localhost", port_number)
finalRdd = users.foreachRDD(resovoier)
ssc.start()
ssc.awaitTermination()