# a)

import re
import string

def wordCount(file):
    text_rdd = scon.textFile("/data/texte/{0}".format(file))
    
    text_rdd = text_rdd.map(lambda x: re.sub(r"[^\w\s]", "", x))
    
    words_rdd = text_rdd.flatMap(lambda x: x.split(" "))
    word_count = words_rdd.countByValue().items()
    word_count_rdd = scon.parallelize(word_count)
    
    return word_count_rdd
    

robinsonCrusoe_rdd = wordCount("robinsonCrusoe.txt")
print(robinsonCrusoe_rdd.takeOrdered(10, key = lambda x: -x[1]))

shakespeare_rdd = wordCount("Shakespeare.txt")
print(shakespeare_rdd.takeOrdered(10, key = lambda x: -x[1]))


# b)


robinson_keys = robinsonCrusoe_rdd.keys().collect()
print("Keys: " + str(robinson_keys))

robinson_values = robinsonCrusoe_rdd.values().collect()
print("Values: " + str(robinson_values))

print("Count: " + str(len(robinson_keys)))

robinson_pairs_sorted_by_key = robinsonCrusoe_rdd.takeOrdered(robinsonCrusoe_rdd.count(), key = lambda x: x[0])
print("Word pairs sorted by key: " + str(robinson_pairs_sorted_by_key))

robinson_pairs_sorted_by_value_desc = robinsonCrusoe_rdd.takeOrdered(robinsonCrusoe_rdd.count(), key = lambda x: -x[1])
print("Word pairs sorted by value: " + str(robinson_pairs_sorted_by_value_desc))


# c)
def countAllWords(rdd):
    return rdd.values().aggregate(0, lambda x, y: x + y, lambda x, y: x + y)

robison_count_all_words = countAllWords(robinsonCrusoe_rdd)
print(robison_count_all_words)


# d)

def normalize(rdd):
    cnt = countAllWords(rdd)
    normalized_rdd = rdd.map(lambda x: (x[0], x[1] / cnt))
    return normalized_rdd


rdd_normalized = normalize(robinsonCrusoe_rdd)
print(rdd_normalized.takeOrdered(10, key = lambda x: -x[1]))


# e)

def replaceNoneTypeInRatio(tup):
    word = tup[0]
    r1 = tup[1][0]
    r2 = tup[1][1]
    if r1 is None:
        r1 = -r2
    if r2 is None:
        r2 = -r1
    return (word, (r1, r2))


def relDist(file1, file2):
    normalized_rdd1 = normalize(wordCount(file1))
    normalized_rdd2 = normalize(wordCount(file2))
    
    
    joined_rdd = normalized_rdd1.fullOuterJoin(normalized_rdd2)
    
    dist_rdd = joined_rdd.map(lambda x: replaceNoneTypeInRatio(x))
    
    
    abs_distance_rdd = dist_rdd.map(lambda x: abs(x[1][0] - x[1][1]))
    return abs_distance_rdd.aggregate(0, lambda x, y: x + y, lambda x, y: x + y)


    
# f)

file_names = ["Das_Schloss.txt", "Der_Prozess.txt", "Effi_Briest.txt", 
              "Shakespeare.txt", "MobyDick.txt", "tomSawyer.txt"]

def distOfTexts(files):
    list_len = len(files)
    for i in range(list_len):
        for j in range(list_len):
            if i != j:
                print("{0} {1} {2}".format(files[i], files[j], str(relDist(files[i], files[j]))) )
                
distOfTexts(file_names)

