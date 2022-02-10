
# a)
rdd1 = scon.range(1, 101, 1)
rdd2 = scon.range(50, 151, 1)

   
# b)
def print_rdd(rdd):
    """"Prints rdd sorted, counnt, first element and a random taken sample of size 10."""
    print("Sorted values in rdd: " + str(rdd.takeOrdered(rdd.count())))
    print("Number of values in rdd: " + str(rdd.count()))
    print("First element inf rdd: " +str(rdd.first()))
    print("Random taken sample of size 10 with no replacemennt" + 
          str(rdd.takeSample(False, 10)))
   
print_rdd(rdd1)
print_rdd(rdd2)


# c)

intersection = rdd1.intersection(rdd2)
intersectionCount = intersection.count()
print("Intersection ordered: " 
      + str(intersection.takeOrdered(intersectionCount)))
print("Element count for intersection: " + str(intersectionCount))


union = rdd1.union(rdd2)
unionCount = union.count()
print("Union sorted: " + str(union.takeOrdered(unionCount)))
print("Element count for union: " + str(unionCount))

difference2from1 = rdd1.subtract(rdd2)
difference2from1Count = difference2from1.count()  
print("Difference rdd1 - rdd2 sorted: " 
      + str(difference2from1.takeOrdered(difference2from1Count)))
print("Element count for rdd1 - rdd2: " + str(difference2from1Count))

difference1from2 = rdd2.subtract(rdd1)
difference1from2Count = difference1from2.count()  
print("Difference rdd2 - rdd1: " +
      str(difference1from2.takeOrdered(difference1from2Count)))
print("Element count for rdd2 - rdd1: " + str(difference1from2Count))

cartesian = rdd1.cartesian(rdd2)
cartesianCount = cartesian.count()
print("Cartesian: " + str(cartesian.takeOrdered(cartesian.count())))
print("Element count for cartesian: " + str(cartesianCount))


# d)

rdd1Mapped = rdd1.map(lambda x : 1/x)
print_rdd(rdd1Mapped)
rdd1MappedSum = rdd1Mapped.fold(0, lambda x, y: x + y)
print("Sum for elements in mapped rdd1:" + str(rdd1MappedSum))

import matplotlib.pyplot as plt

rdd1MappedArray = rdd1Mapped.collect()
rdd1Array = rdd1.collect()

fig, ax = plt.subplots()
ax.plot(rdd1Array, rdd1MappedArray)
ax.set_xlabel("Rdd1")
ax.set_ylabel("Mapped Rdd1")
plt.show()

# e)
rdd2Product = rdd2.fold(1, lambda x, y: x * y)

print("Product of all elements in rdd2: " + str(rdd2Product))

# f)
rdd1ElementsDivBy7Or5 = rdd1.filter(lambda x: x % 5 == 0 and x % 7 == 0)
print_rdd(rdd1ElementsDivBy7Or5)