# a)
text_rdd = scon.textFile("/data/texte/robinsonCrusoe.txt") 


# b) 
char_count = text_rdd.map(lambda x: len(x)).reduce(lambda x, y: x + y)
print(char_count)


# c)
distinct_char_dist = text_rdd.flatMap(lambda x: x).countByValue().items()
distinct_char_count_rdd = scon.parallelize(distinct_char_dist)
print(distinct_char_count_rdd.takeOrdered(distinct_char_count_rdd.count(),
                                      key=lambda x: -x[1] ))


# d)

import re

def wordCount(file, top=None):
    text_rdd = scon.textFile("/data/texte/{0}".format(file))
    
    text_rdd = text_rdd.map(lambda x: re.sub(r"[^\w\s]", "", x))
    
    words_rdd = text_rdd.flatMap(lambda x: x.split(" "))
    word_count = words_rdd.countByValue().items()
    word_count_rdd = scon.parallelize(word_count)
    top = top if top else word_count_rdd.count()
    print(word_count_rdd.takeOrdered(top, 
                                     key = lambda x: -x[1]))
    
# d) / e)
wordCount("robinsonCrusoe.txt", top=10)
wordCount("tomSawyer.txt", top=10)
wordCount("Ulysses.txt", top=10)
