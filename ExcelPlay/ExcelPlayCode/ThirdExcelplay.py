'''
Created on Jul 7, 2017

@author: stata001c
'''

import xlrd
import os.path

loop_num = 1
temp = ''
fname = 'C:/Users/stata001c/Desktop/CTS-innovations/Table_name_subject_area_specs.xlsx'
xl_workbook = xlrd.open_workbook(fname)
xl_sheet = xl_workbook.sheet_by_index(0)
max_col_idx = xl_sheet.ncols

str3 = ''

for row_idx in range(0,xl_sheet.nrows):
    if xl_sheet.row_types(row_idx,start_colx=0,end_colx=None)[0] > 0 and xl_sheet.row_types(row_idx,start_colx=0,end_colx=None)[1] > 0:
        
        if loop_num == 1:
            if xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[0].startswith("("):
                dummy = 1
            else:
                str3 += "<h1>Table Name- %s</h1><h3>Subject area- %s</h3> \n" % (xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[0].strip()\
                                                                              ,xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[1].strip())
                table_name = xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[0].strip()
        if loop_num == 2:
            if xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[0].startswith("("):
                dummy = 1
            else:
                str3 += "<p>%s</p>" % xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[0].strip()
        if loop_num == 3:
            if xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[0].startswith("("):
                str3 += "<h3>Contact&nbsp;information-</h3> \
                <table style=\"height: 78px;\" border=\"1\" width=\"681\">\
                <tbody> \n"
            else:
                str3 += "<tr><td>%s</td> <td> %s </td> <td>%s</td> </tr> \n" % (xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[0].strip()\
                                                                              ,xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[1].strip()\
                                                                              ,xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[2].strip())
                        
        if loop_num == 4:
            if xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[0].startswith("("):
                dummy = 1
                str3 += "</tbody> </table> \n <h3>&nbsp;</h3><h3>ETL Details-</h3><table style=\"height: 66px;\" border=\"1\" width=\"685\"><tbody><tr>\
                <td><strong>Script name</strong></td><td><strong>Path</strong></td></tr> \n" \
                
            else:
                str3 += "<tr><td>%s</td>\
                <td>%s</td></tr>" % (xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[0].strip()\
                                    ,xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[1].strip())
                
        if loop_num == 5:
            if xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[0].startswith("("):
                dummy = 1
                str3 += "</tbody></table> \n <h3>Load details-</h3><table style=\"height: 59px;\" border=\"1\" width=\"684\"><tbody><tr>\
                <td><strong>Load Frequency</strong></td><td><strong>Object_Type</strong></td><td><strong>Environment</strong></td></tr> \n"
            else:
                str3 += "<tr><td>%s</td><td>%s</td><td>%s</td></tr> \n" % (xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[0].strip()\
                                                                              ,xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[1].strip()\
                                                                              ,xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[2].strip())
                
        if loop_num == 6:
            if xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[0].startswith("("):
                dummy = 1
                str3 += "</tbody></table> \n" # this is closing block for prior one
                str3 += "<h3>Metadata-</h3><table style=\"height: 192px;\" border=\"1\" width=\"528\"><tbody><tr> \
                <td><strong>Column Name</strong></td><td><strong>Data Type</strong></td>\
                <td><strong>Nullable</strong></td><td><strong>Definition</strong></td></tr> \n"




                 
            else:
                str3 += "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr> \n" %(xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[0].strip()\
                                                                                 ,xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[1].strip()\
                                                                                 ,xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[2].strip()\
                                                                                 ,xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[3].strip())

#                str3 += "<tr style=\"height: 52px;\"><td style=\"width: 210px; height: 52px;\">%s</td><td style=\"width: 93px; height: 52px;\">%s</td>\
#                <td style=\"width: 57px; height: 52px;\">%s</td><td style=\"width: 492px; height: 52px;\">%s</td></tr> \n" % (xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[0].strip()\
#                 ,xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[1].strip()\
#                 ,xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[2].strip()\
#                 ,xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[3].strip())

                                
    elif xl_sheet.row_types(row_idx,start_colx=0,end_colx=None)[0] == 0 or xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[0].strip() == '':
        loop_num+=1
str3 += "</tbody></table> \n"      
out_file = "C:/Users/stata001c/Desktop/CTS-innovations/%s_v1.html" % table_name.lower()
print (out_file)
text_file = open(out_file, "w")
text_file.write(str3)
text_file.close()

if os.path.isfile(out_file):
    print ("+++++ Success ++++++")
else:
    print ("something wrong, program has failed")    
