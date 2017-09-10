'''
Created on Aug 30, 2017

@author: stata001c
'''
s = "hello world!"
#s[0:1] = '\''

print(iter(s))
print(s[:5])
print(s[0:3])
print (s[-1])










'''

def func2(*args, **kwargs): pass
def func3(a1,a2, **kwargs): pass
def func4(yoo='hoo', **kwargs): pass
def func5(a='default', **kwargs): pass
#def func6(*args, yoo='hoo'): pass

print (2 ** 3)
print (2 # 3)
print (2 ~ 3)
print (2 << 3)
'''


a = [1,2,3,4,5]
for ab in range(0,len(a)):
    print(a[ab])
kv = [{1,'a'},{2,'b'}]
for ii in kv:
    for k,v in ii.iteritems():
        print(k +" " +v)
            
            
def func(b,*a,**az):
    for i in a:
        print(i)
    y = [n for n in b if n % 2 == 1]
    print(y)
print(func(a,'aa','bbb'))
