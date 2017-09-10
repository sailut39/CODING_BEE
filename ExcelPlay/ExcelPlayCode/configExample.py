import configparser

dir_pa = 'C:/old profile/Desktop/Sailus/Spark/workspace'
ini_file = 'ini_file.txt'

cfg = configparser.RawConfigParser()
cfg.read(dir_pa+"/"+ini_file)
name1 = cfg.get('GLOBAL','name')
age1 = cfg.get('GLOBAL','age')
print (name1)
print (age1)
print(int(age1))