import itertools
from pyspark import SparkContext
import time
import sys
import math

def read_input(path):
    data = sc.textFile(path)
    head = data.first()
    data = data.filter(lambda x: x != head).map(lambda x: x.split(','))
    return data


def build_out_string(input_list):
    out = ""
    size=1
    for item in input_list:
        l = len(item)
        if l == size:
            if size ==1:
                out += str(item).replace(',', '') + ','
            else:
                out += str(item)+','
        else:
            out=out.rstrip(',')
            out+='\n\n'
            out+=str(item)+','
            size+=1


    return out.rstrip(',')

def write_output(out1,out2,output_path):
    output="Candidates:\n"
    output+=out1 +"\n"
    output+="\nFrequent Itemsets:"+"\n"
    output += out2
    with open(output_path,"w") as fout:
        fout.write(output)


def apriori(support_threshold, basket, totalsize):


    baskets = list(basket)
    reduced_threshold = math.ceil(support_threshold * (float(len(baskets)) / float(totalsize)))
    frequent_itemset = list()
    frequent_items = set()
    singleton = {}

    for basket in baskets:
        for item in basket:
            if item in singleton:
                if singleton[item] < reduced_threshold:
                    singleton[item] += 1
                    if (singleton.get(item) >= reduced_threshold):
                        frequent_items.add(item)
            else:
                singleton[item] = 1


    if frequent_items:
        candidate_set = frequent_items
        for i in frequent_items:
            frequent_itemset.append(tuple([i]))



    candidate_set = itertools.combinations(candidate_set, 2)

    paircount = {}
    frequent_items.clear()

    for item in candidate_set:
        itemset = set(item)
        for basket in baskets:
            if itemset.issubset(basket):
                if item in paircount:
                    if paircount[item] < reduced_threshold:
                        paircount[item] += 1
                        if (paircount.get(item) >= reduced_threshold):
                            item = tuple(sorted(item))
                            frequent_items.add(item)
                else:
                    paircount[item] = 1

    candidate_set = frequent_items
    if frequent_items:
        frequent_itemset.extend(frequent_items)
    size=3
    while frequent_items:
            candidate_set = list(set(itertools.chain.from_iterable(candidate_set)))
            candidate_set = itertools.combinations(candidate_set, size)
            frequent_items.clear()
            candidatecount = {}
            for item in candidate_set:
                itemset = set(item)
                for basket in baskets:

                    if itemset.issubset(basket):
                        if item in candidatecount:
                            if candidatecount[item] < reduced_threshold:
                                candidatecount[item] += 1
                                if(candidatecount.get(item) >= reduced_threshold):
                                    item = tuple(sorted(item))
                                    frequent_items.add(item)
                        else:
                            candidatecount[item] = 1

            candidate_set = frequent_items
            if frequent_items:
                frequent_itemset.extend(frequent_items)

            size += 1
    return frequent_itemset


def finalCandidatesCount(baskets, frequentcandidates):
    finalcounts = {}
    baskets = list(baskets)
    for candidate in frequentcandidates:
        candidateset = set(candidate)
        for basket in baskets:
            if candidateset.issubset(basket):
                if candidate in finalcounts:
                    finalcounts[candidate] += 1
                else:
                    finalcounts[candidate] = 1

    return finalcounts.items()


def find_frequent(data, support_threshold, filter_threshold,output_path):
    rdd = data.map(lambda x: (x[0], x[1]))
    filtered_rdd = rdd.groupByKey().filter(lambda x: len(x[1]) > filter_threshold)
    set_rdd= filtered_rdd.mapValues(set)
    rdd= set_rdd.coalesce(4)
    rdd = rdd.map(lambda x: x[1]).cache()
    total = rdd.count()
    map1_1 = rdd.mapPartitions(lambda basket: apriori(support_threshold, basket, total))
    map1_2 = map1_1.map(lambda x: (x, 1))
    reduce1 = map1_2.reduceByKey(lambda x,y: (1)).keys().collect()
    reduce1.sort(key=lambda x: [len(x), x])
    out1 = build_out_string(reduce1)
    map2 = rdd.mapPartitions(lambda basket: finalCandidatesCount(basket, reduce1))
    reduce2 = map2.reduceByKey(lambda x, y: x+y).filter(lambda x: x[1] >= support_threshold).keys().collect()
    reduce2.sort(key=lambda x: [len(x), x])
    out2= build_out_string(reduce2)
    write_output(out1,out2,output_path=output_path)




start = time.time()
sc = SparkContext()
filter_threshold = int(sys.argv[1])
support_threshold = int(sys.argv[2])
input_path = sys.argv[3]
output_path = sys.argv[4]
data = read_input(input_path)
find_frequent(data, support_threshold, filter_threshold,output_path)
print("Duration:", time.time() - start)