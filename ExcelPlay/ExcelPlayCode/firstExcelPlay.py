'''
Created on Jul 7, 2017

@author: stata001c
'''

import pandas as pd
import xlrd

'''
df = pd.DataFrame({'Data': [10, 20, 30, 20, 15, 30, 45]})

#C:/Users/stata001c/AppData/Local/Programs/Python/Python36-32/temp/

# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter('C:/Users/stata001c/AppData/Local/Programs/Python/Python36-32/temp/123_sample.xlsx', engine='xlsxwriter')

# Convert the dataframe to an XlsxWriter Excel object.
df.to_excel(writer, sheet_name='Sheet1')

# Close the Pandas Excel writer and output the Excel file.
writer.save()

'''
fname = 'C:/Users/stata001c/AppData/Local/Programs/Python/Python36-32/temp/123_sample.xlsx'
xl_workbook = xlrd.open_workbook(fname)
sheet_names = xl_workbook.sheet_names()
print('Sheet Names', sheet_names)
xl_sheet = xl_workbook.sheet_by_name(sheet_names[0])

# Or grab the first sheet by index 
#  (sheets are zero-indexed)
#
xl_sheet = xl_workbook.sheet_by_index(0)
print ('Sheet name: %s' % xl_sheet.name)
for row_idx in range(0,xl_sheet.nrows):
    row = xl_sheet.row(row_idx)
    print (xl_sheet.row_values(row_idx)[0]+"|"+ str(xl_sheet.row_values(row_idx)[1]))
    print (str(xl_sheet.row_types(row_idx)[0])+"|"+ str(xl_sheet.row_types(row_idx)[1]))
#    for col_idx in range(0,xl_sheet.ncols):
#            print (xl_sheet.row_values(row_idx, start_colx=0, end_colx=None)[0])


#    print (row)s
#    print (row[row_idx])







str1 = "(this is string example....wow!!! this is really string"

print (str1.startswith("("))


print (str1.replace("is", "was"))
print (str1.replace("is", "was", 3))