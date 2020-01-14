import json
from pyspark import SparkContext
import sys
import time

start_time=time.time()
input_file=sys.argv[1]
output_file=sys.argv[2]

output={ 'n_review': None ,
         'n_review_2018': None,
         'n_user': None,
         'top10_user': list(),
         'n_business': None,
         'top10_business':None}


sc = SparkContext()
input_rdd = sc.textFile(input_file)
rdd =input_rdd.map(json.loads)


output['n_review']=rdd.count()

result_2018 = rdd.map(lambda x : (x['date'].split('-')[0],1)).filter(lambda x:x[0]=='2018').reduceByKey(lambda x,y:x+y).collect()
output['n_review_2018']=result_2018[0][1]
output['n_user'] = rdd.map(lambda x:x['user_id']).distinct().count()
top10_users= rdd.map(lambda x: (x['user_id'],1)).reduceByKey(lambda x,y:x+y).sortByKey().takeOrdered(10,lambda x: -x[1])
#top10_users= rdd.map(lambda x: (x['user_id'],1)).reduceByKey(lambda x,y:x+y).takeOrdered(10,lambda x: -x[1])
output['top10_user']=list(map(list,top10_users))
output['n_business'] = rdd.map(lambda x : x['business_id']).distinct().count()
top10_business= rdd.map(lambda x: (x['business_id'],1)).reduceByKey(lambda x,y:x+y).sortByKey().takeOrdered(10,lambda x: -x[1])
#top10_business= rdd.map(lambda x: (x['business_id'],1)).reduceByKey(lambda x,y:x+y).takeOrdered(10,lambda x: -x[1])
output['top10_business']=list(map(list,top10_business))
with open(output_file,'w') as fout:
    fout.write(json.dumps(output))
print("Duration= " + str(time.time()-start_time))