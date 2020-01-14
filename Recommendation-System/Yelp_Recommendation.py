# Method Description:
# Implemented Hybrid collaborative filtering using Alternating Least Squares (ALS) Matrix Factorization and SVD
# leveraging Surprise package for recommendation systems. The algorithm's compute low-rank matrix factorization which solves popular bias and item cold-start problems in collaborative filtering.
# The weighted score was taken to get final scores ( 0.9 * ALS score + 0.1 * SVD )
#
#
#
# The following parameters were tuned:
# Surprise BaselineOnly= als
# parameters 'n_epochs': 30
# reg_u: 6 (The regularization parameter for user)
# reg_i: 4 (The regularization parameter for business)
#
# SVD (n_factors=105, n_epochs=55, lr_all=0.005, reg_all=0.22 )
#
# Error Distribution:
# >=0 and <1: 100343
# >=1 and <2: 34220
# >=2 and <3: 6644
# >=3 and <4: 836
# >=4: 1
#
# RSME:
# 0.9998801249132381
#
# Execution Time:
# 84.70196771621704s

import time
import sys
from surprise import Reader, Dataset, SVD, BaselineOnly
from pyspark import SparkContext

sc = SparkContext()
sc.setLogLevel("WARN")
start = time.time()


mode='train'
rdd = sc.textFile(sys.argv[2])
header = rdd.first()
rdd = rdd.repartition(4)
testrdd = rdd.filter(lambda x: x != header).map(lambda x: x.split(','))
if len(header.split(',')) == 3:
    testrdd = testrdd.map(lambda x: (x[0],x[1],float(x[2]))).collect()
else:
    testrdd = testrdd.map(lambda x: (x[0],x[1])).collect()
    mode = 'test'

def als_predictions(trainset,dataset_test):
    algo = BaselineOnly(bsl_options={'method': 'als', 'n_epochs': 30, 'reg_u': 6, 'reg_i': 4})
    predictions = algo.fit(trainset)
    list_1=[]
    for x in dataset_test:
        i = predictions.predict(x[0],x[1]) if mode == 'test' else predictions.predict(x[0],x[1],x[2])
        list_1.append((i[0], i[1], i[2], i[3]))
    return  list_1

def svd_predictions(trainset,dataset_test):
    algo = SVD(n_factors=105, n_epochs=55, lr_all=0.005, reg_all=0.22)
    predictions = algo.fit(trainset)
    list_2=[]
    for x in dataset_test:
        i = predictions.predict(x[0],x[1]) if mode == 'test' else predictions.predict(x[0],x[1],x[2])
        list_2.append((i[0], i[1], i[2], i[3]))
    return  list_2


def calculate_counts(difference):
    if (difference >= 0 and difference < 1):
        val= (0, 1)
    elif (difference >= 1 and difference < 2):
        val= (1, 1)
    elif (difference >= 2 and difference < 3):
        val= (2, 1)
    elif (difference >= 3 and difference < 4):
        val= (3, 1)
    elif (difference >= 4):
        val= (4, 1)
    return val

def build_output_description(rootmeansqerr, resdiff):
    print("Writing to description file: harsh_waghela_description\n")
    print("RMSE: "+str(rootmeansqerr))
    print("Time:"+str(time.time() - start) + "s")
    out=""
    out+= "Method Description:\n"
    out+= "The objective of this project\n"
    out+="Error Distribution:\n"
    out+=">=0 and <1: " + str(resdiff[0][1])+"\n"
    out+=">=1 and <2: " + str(resdiff[1][1])+"\n"
    out+=">=2 and <3: " + str(resdiff[2][1])+"\n"
    out+=">=3 and <4: " + str(resdiff[3][1])+"\n"
    out+=">=4: " + str(resdiff[4][1])+"\n\n"
    out+="RSME:\n"
    out+=str(rootmeansqerr)+"\n\n"
    out+="Execution Time:\n"
    out+=str(time.time() - start) + "s"
    with open("harsh_waghela_description.txt", "w") as fin:
        fin.write(out)

def build_prediction_outputs(resA):
    print("Writing output to: "+sys.argv[3])
    out="user_id, business_id, prediction\n"
    for i in resA.collect():
        out+= str(i[0]) + "," + str(i[1]) + "," + str(i[2])+"\n"
    with open(sys.argv[3], "w") as fin:
        fin.write(out)

trainset = Dataset.load_from_file(sys.argv[1]+'yelp_train.csv', Reader(sep=',', skip_lines=1,line_format='user item rating',rating_scale=(1, 5)))
trainset = trainset.build_full_trainset()
predList1 = als_predictions(trainset,testrdd)
predList2 = svd_predictions(trainset,testrdd)
pred = sc.parallelize(predList1, numSlices=1000).map(lambda x: ((x[0],x[1]),(x[2],x[3])))
pred = pred.repartition(4)
pred2 = sc.parallelize(predList2, numSlices=1000).map(lambda x: ((x[0],x[1]),(x[2],x[3])))
pred2 = pred2.repartition(4)
joined = pred.join(pred2)
joined =   joined.map(lambda x: (x[0][0],x[0][1],x[1][0][0],0.9* float(x[1][0][1])+0.1* float(x[1][1][1])))

if mode=='train':
    final_values = joined.map(lambda x: (x[0],x[1],x[3],abs(x[2]-x[3])))
else:
    final_values = joined.map(lambda x: (x[0],x[1],x[3]))

if mode=='train':
    absdiff = final_values.map(lambda x: x[3])
    resdiff = absdiff.\
        map(calculate_counts).\
        reduceByKey(lambda x, y: x + y).\
        sortByKey()\
        .collect()
    rootmeansqerr = pow(absdiff.map(lambda x: x * x).mean(), 0.5)
    print("RMSE: "+str(rootmeansqerr))
    print("Time:"+str(time.time() - start) + "s")
    build_output_description(rootmeansqerr, resdiff)

build_prediction_outputs(final_values)
