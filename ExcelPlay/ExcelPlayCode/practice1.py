'''
Created on Aug 28, 2017

@author: stata001c
'''

'''
import pickle

dir_name = 'C:\old profile\Desktop\Sailus\Spark\workspace'

data1 = {'a': [1, 2.0, 3, 4+6j],
         'b': ('string', u'Unicode string'),
         'c': None}

selfref_list = [1, 2, 3]
selfref_list.append(selfref_list)

output = open(dir_name+'\data.pkl', 'wb')

# Pickle dictionary using protocol 0.
pickle.dump(data1, output)

# Pickle the list using the highest protocol available.
pickle.dump(selfref_list, output, -1)

output.close()

# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
import pprint

pkl_file = open(dir_name+'\data.pkl', 'rb')

data1 = pickle.load(pkl_file)
pprint.pprint(data1)

data2 = pickle.load(pkl_file)
pprint.pprint(data2)

pkl_file.close()

#+++++++++++++++++++++++

i = input('give a number:')
print(i)

'''
'''
# List content of a directory

import os 
from pathlib import Path

def print_directory_contents(sPath):
    if sPath.exists() and sPath.is_dir():
        for i in sPath.rglob('*'):
            print(i)
my_path = Path("C:\old profile\Desktop\Sailus\Spark\workspace")
print_directory_contents(my_path)
'''
'''
# +++ Interactive program to get inputs and dispaly it on screen

import time

name = input("Enter your name :")
age = input("Enter your age :")

remain_age = 100 - int(age)

print("name: %s, age: %s, reamin_age: %s "%(name,str(age),str(remain_age)))

curr_year = time.strftime("%Y")

result_year = int(curr_year) + remain_age

print ("Hello %s, hope you had a nice day!! \nyou are reaching to 100 in %s years!" %(name, str(result_year)))
'''
'''
number = input("Please input a number: ")
number = int(number)
fur_num = None
fur_div_num = None
def even_odd(num):    
    if num%2 == 0:
        print("it is an even number!!")
    else:
        print("odd number")

def further():
    fur_num = input("Hey, lets check further, enter any number: ")
    fur_num = int(fur_num)
    fur_div_num = input("provide a number with which the previous one has to be divide with : ")
    fur_div_num = int(fur_div_num)
    if (fur_num / fur_div_num)%2 == 0:
        print("Good news!! the number %s is evenly dividable with %s" %(str(fur_num),str(fur_div_num)))
    else:
        print("Appears the number combination is not good!!")
    
    
if number%4 == 0:
    print("entered number is multiples of 4, it is sum of %s times of 4" %(str(number/4)))
    even_odd(number)
else:
    even_odd(number)
further()
'''


a = [1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89]
new_list = []
res = []
for i in range(0,5):
    new_list.append(a[i])

print (new_list)

inp = input("Provide a number to get all small elements from list: ")
inp = int(inp)

for i in range(0,len(a)):
    if a[i] < inp:
        res.append(a[i])
print("list of elements less than given number are: " + str(res))
    