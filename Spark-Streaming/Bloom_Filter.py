from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import sys
import binascii
from datetime import datetime
import os


def myhashs(user):
    no_of_hashes = 8
    global bit_array_size
    huser= int(binascii.hexlify(user.encode('utf8')), 16)
    hashes = []
    for i in range(1, no_of_hashes+1):
        current_hash_code = ((i * huser) + (i * 13)) % bit_array_size
        hashes.append(current_hash_code)
    return hashes


def bloom_filter(current_stream):
    global seen_users
    global false_positive_count
    global user_count
    global out_file



    curr_user_stream = current_stream.collect()

    for user in curr_user_stream:
        flag = False
        hashes = myhashs(user)
        user_count += 1

        for hashcode in hashes:
            if bloom_filter_bit_array[hashcode] == 0:
                flag = True
                bloom_filter_bit_array[hashcode] = 1


        if not flag:
            if user not in seen_users:
                false_positive_count += 1

        seen_users.add(user)

    false_positive_rate = float(false_positive_count) / float(user_count)
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    out_file.write(str(current_timestamp) + "," + str(false_positive_rate) + "\n")
    out_file.flush()
    os.fsync(out_file.fileno())
    return



port_number = int(sys.argv[1])
output_file_path = sys.argv[2]

sc = SparkContext()
ssc = StreamingContext(sc, 10)
bit_array_size=69997
bloom_filter_bit_array = [0] * bit_array_size
user_count = 0
false_positive_count = 0

seen_users = set()


with open(output_file_path, "w") as out_file:
    out_file.write("Time,FPR\n")

out_file = open(output_file_path,"a+")
users = ssc.socketTextStream("localhost", port_number)
finalRdd = users.foreachRDD(bloom_filter)


ssc.start()
ssc.awaitTermination()
out_file.close()
