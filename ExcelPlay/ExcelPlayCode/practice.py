'''
Created on Aug 28, 2017

@author: stata001c
'''
res = []
def fib(n):
    a,b = 0,1
    print(a)
    while b < n:
        print (b)
        a,b = b, a+b
       
# fib(10)

def fib2(n):
    a,b = 0,1
    res = []
    res.append(a)
    while b < n:
        res.append(b)
        a, b = b, a + b
    return res

res = fib2(10)

print(__name__)