import avro, os

b = []
for i in os.walk('C:\old profile\Desktop\Sailus\Spark\workspace\problem1\orders'):
    b.append(i)
    
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

dir_name = 'C:\old profile\Desktop\Sailus\Spark\workspace\problem1\orders'

schema = avro.schema.parse(open(dir_name+"/part-m-00000.avro", "rb").read())

print(schema)

'''
reader = DataFileReader(open(dir_name+"/part-m-00000.avro", "rb"), DatumReader())
for user in reader:
    print user
reader.close()
'''