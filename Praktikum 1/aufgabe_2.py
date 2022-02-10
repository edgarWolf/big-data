#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 13 19:12:06 2021

@author: wolfedgar
"""

import matplotlib.pyplot as plt
import time
import math
# Aufgabe 2

# a)

# Summe
test_sum = 3 + 4
print("Sum = " + str(test_sum))

# Differenz
diff = 4 - 3
print("Differennce = " + str(diff))

# Multiplikation
mul = 3 * 4
print("Multiplication = " + str(mul))

# Division
div = 7 / 3
print("Division = " + str(div))

# Modulo
mod = 5 % 3
print("Modulo = " + str(mod))

# Potenz
power = 2**5
print("Modulo = " + str(power))


# b)
test_list = [1, 4, "test", 3.6, True]
print("Initial list: "+ str(test_list))

first_element = test_list[0]
print("First element:" + str(first_element))


last_element = test_list[len(test_list) - 1]
print("Last element:" + str(last_element))

elements_starting_index_3 = test_list[3:]
print("Elements from index 3: " + str(elements_starting_index_3))

elements_2_until_4 = test_list[2:4]
print("Elements from index 2 until index 4: " + str(elements_2_until_4))

elemens_in_steprange_of_2 = test_list[0:5:2]
print("Elements from index 2 until index 5 with steprange of 2: " + str(elements_2_until_4))

test_list.insert(3, "inserted")
print("List after insertion: " + str(test_list))

del test_list[5]
print("List after deletion: " + str(test_list))

test_list.append((16, False))
print("List after append: " + str(test_list))

# c)
test_range = range(1000)

test_range_sum = 0
for number in test_range:
    test_range_sum += number
print("Sum for all numbers from 0 until 999: " + str(test_range_sum))


# d)
test_dict = {
    "key": 12,
    14: "value",
    "pi": 3.141
    }

test_value = test_dict["key"]
print("Value with key 'key': "+ str(test_value))

test_dict["new Key"] = 42
print("Value after insertion of new key: " + str(test_dict))

# e)
def get_five_tuple(number_list: list) -> tuple:
    """Returns a five tuple (min, max, avg, count, sum) for a given list."""
    min_value = min(number_list)
    max_value = max(number_list)
    avg_value = sum(number_list) / len(number_list)
    count = len(number_list)
    sum_value = sum(number_list)    
    return (min_value, max_value, avg_value, count, sum_value)

input_list = range(1000)
tuple_result = get_five_tuple(input_list)
print("Five tuple for first 1000 numbers: " + str(tuple_result))

# f)
comprehension_result = [x for x in range(1, 10001, 1) if x % 3 == 0 
                        or x % 5 == 0 or x % 7 == 0]
tuple_result = get_five_tuple(comprehension_result)
print("Five tuple for first 10000 numbers (with conditions above): " + str(tuple_result))


# g)

def plot_function():
    """Plots the curve fir the function givenn in g)."""
    xv = []
    sv = []
    cv = []
    for i in range(720):
        xv.append(i)
        sv.append(math.sin(i * math.pi / 180) + 
                  math.sin(2 * i * math.pi / 180) / 2 +
                  math.sin(4 * i * math.pi / 180) / 4 +
                  math.sin(8 * i * math.pi / 180) / 8) 
    
        cv.append(math.cos(i * math.pi / 180) + 
                  math.cos(2 * i * math.pi / 180) / 2 +
                  math.cos(4 * i * math.pi / 180) / 4 +
                  math.cos(8 * i * math.pi / 180) / 8) 
         
    fig, ax = plt.subplots()
    ax.plot(xv, sv, 'b', label='Sinus')
    ax.plot(xv, cv, 'r', label='Cosinus')
    ax.legend(loc='lower center')
    plt.title('Sinus und Cosinus-Funktion')
    plt.xlabel('Grad')
    plt.ylabel('Wert')
    plt.show()
    
# h) 

cat_cache = []

def get_cat_number_from_cache(i):
    """"Gibt die i-te catalansche Zahl aus dem Cache zurück, wenn vorhanden."""
    if (len(cat_cache) <= i):
        cat_cache.append(catalnsche_zahlen(i))
    return cat_cache[i]

def catalnsche_zahlen(n):
    """Computes the cataln numbers up until n."""
    # Rekursive Lösung, sehr langsam.
    if n == 0:
        return 1

    result = 0
    for k in range(n):
        first_number = get_cat_number_from_cache(k)
        second_number = get_cat_number_from_cache(n - k - 1)
        result += first_number * second_number
    
    return result


def get_cat_numbers_fully_recursive(n, values=[]):
    """All in one Lösung für die catalanschen Zahlen."""
    values_count = len(values)
    # Die 1 nur initial einsetzen.
    
    if n == 0 and values_count == 0:
        values.append(1)
        return values
    
    # Alle bereits berechneten Werte einfach zurückgeben
    if values_count > n:
        return values
    
    # Summe nach Vorschrift bilden und in die Liste einfügen.
    result = 0
    for k in range(n):
        # Index first number: 0,1,...,n
        # Index second number: n, n-1,...0
        first_number = get_cat_numbers_fully_recursive(k, values)[k]
        second_number = get_cat_numbers_fully_recursive(n - k - 1, values)[n-k-1]
        result += first_number * second_number
    values.append(result)
    return values


# h & i)


time_values = []
def get_catalansche_zahlen_as_list(start, stop, step=1):
    """Berechnet catalansche Zahlen im angegebenen Intervall."""
    cat_cache.clear()
    result = []
    for i in range(start, stop + 1, step):
        start = time.process_time()
        result.append(catalnsche_zahlen(i))
        elapsed_time = time.process_time() - start
        time_values.append(elapsed_time)
    return result

def measure_time_recursive_cat_function(start, stop, step=1):
    elapsed_times = []
    for i in range(start, stop + 1, step):
        start = time.process_time()
        catalnsche_zahlen(i)
        elapsed_time = time.process_time() - start
        elapsed_times.append(elapsed_time)
    return elapsed_times

# i)



def scatter_plot_time(time_values):
    """Helper function for scattering elapsed time values for previous function."""
    xv = []
    yv = []
    for i in range(len(time_values)):
        xv.append(i)
        yv.append(time_values[i])
    plt.scatter(xv, yv)
    
    
def line_plot_time(time_values):
    """Helper function for plotting elapsed time values for previous function."""
    xv = []
    yv = []
    for i in range(len(time_values)):
        xv.append(i)
        yv.append(time_values[i])
    plt.plot(xv, yv)


scatter_plot_time(measure_time_recursive_cat_function(50, 1500, 50))
line_plot_time(measure_time_recursive_cat_function(50, 1500, 50))

# j)


# Path: /data/adsb/adsbprak.txt
def line_plot_airplane_data(filename):
    """Plots the ariplane data from the given file."""
    plane_data = {}
    
    with open(filename) as f:
        for line in f:
            line_values = line.split(",")
            plane_id = line_values[0]
            
            if plane_id not in plane_data:
                plane_data[plane_id] = ([], [])
            
            latidude = float(line_values[4])
            longitude = float(line_values[5])
            plane_data[plane_id][0].append(latidude)
            plane_data[plane_id][1].append(longitude)
            
    fig, ax = plt.subplots()
    
    plane_keys = plane_data.keys()
    for plane in plane_keys:
        ax.plot(plane_data[plane][0], plane_data[plane][1])
        ax.scatter(plane_data[plane][0], plane_data[plane][1])
    plt.show()

line_plot_airplane_data("/data/adsb/adsbprak2.txt")  

        

    
            
  