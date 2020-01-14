from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import sys,os
import math
import binascii
from datetime import datetime
from statistics import mean, median


port_number = int(sys.argv[1])
output_file_path = sys.argv[2]




def myhashs(user):
    global a,b
    user_hashed = int(binascii.hexlify(user.encode('utf8')), 16)
    hashes=[]
    for i in range(num_hashes):
        current_hash_code = (((a[i] * user_hashed) + b[i]) % 1398318121) % binary_size
        hashes.append(current_hash_code)
    return hashes

def get_trail_zeros(current_hash_code):
    current_hash_code_binary = bin(current_hash_code)[2:]
    return len(current_hash_code_binary) - len(current_hash_code_binary.rstrip("0"))


def flajolet_martin(current_stream):
    global fout
    global num_hashes
    global binary_size
    grouped_averages = []
    current_index = 0
    curr_user_stream = current_stream.collect()
    actual_users = len(set(curr_user_stream))
    max_trailing_zeros = -1
    hash_2r = []


    for i in range(num_hashes):
        for user in curr_user_stream:
            user_hashed = int(binascii.hexlify(user.encode('utf8')),16)
            current_hash_code = (((a[i] * user_hashed) + b[i]) % 1398318121) % binary_size
            trailing_zeros = get_trail_zeros(current_hash_code)
            max_trailing_zeros = trailing_zeros if trailing_zeros > max_trailing_zeros else max_trailing_zeros

        hash_2r.append((pow(2,max_trailing_zeros)))
        max_trailing_zeros = -1


    for group_range in range(2, num_hashes+1, 2):
       group_hashes = hash_2r[current_index:group_range]
       grouped_averages.append(mean(group_hashes))
       current_index = group_range

    expected_count = median(grouped_averages)
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fout.write(str(current_timestamp) + "," + str(actual_users) + "," + str(expected_count) + "\n")
    fout.flush()
    os.fsync(fout.fileno())
    return

b = [11551,11579,11587, 11689, 11699,17471, 17443,17449,16253,17467]
window_length = 30
sliding_interval = 10
num_hashes = 10
binary_size = pow(2, num_hashes)
a = [2269, 1201, 1213, 2423, 2837,18413 ,19319, 18379, 18397, 18401]

sc = SparkContext()
ssc = StreamingContext(sc, 5)

with open(output_file_path,"w") as fout:
    fout.write("Time,Ground Truth,Estimation\n")

fout = open(output_file_path, "a+")
users = ssc.socketTextStream("localhost", port_number)

final = users\
    .window(window_length,sliding_interval)\
    .foreachRDD(flajolet_martin)

ssc.start()
ssc.awaitTermination()
fout.close()