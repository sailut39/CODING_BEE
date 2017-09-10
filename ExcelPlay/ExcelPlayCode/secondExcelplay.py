'''
Created on Jul 7, 2017

@author: stata001c
'''

import xlrd


loop_num = 1
loop3 = 1
loop5 = 1
loop6 = 1
temp = ''
fname = 'C:/Users/stata001c/Desktop/CTS-innovations/Table_name_subject_area_specs.xlsx'
xl_workbook = xlrd.open_workbook(fname)
xl_sheet = xl_workbook.sheet_by_index(0)
max_col_idx = xl_sheet.ncols
row=[]
str1 = open('C:/Users/stata001c/Desktop/CTS-innovations/hotbox.html', 'r').read()

for row_idx in range(0,xl_sheet.nrows):
#    if xl_sheet.row_types(row_idx,start_colx=0,end_colx=None)[0] > 0 and xl_sheet.row_types(row_idx,start_colx=0,end_colx=None)[1] == 0:
#        print ("parsing has been started for block %s"% xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[0])
#        row.append(xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[0])
#    elif xl_sheet.row_types(row_idx,start_colx=0,end_colx=None)[0] > 0 and xl_sheet.row_types(row_idx,start_colx=0,end_colx=None)[1] > 0:
    if xl_sheet.row_types(row_idx,start_colx=0,end_colx=None)[0] > 0 and xl_sheet.row_types(row_idx,start_colx=0,end_colx=None)[1] > 0:
        
        if loop_num == 1:
            if xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[0].startswith("("):
                dummy = 1
            else:
                str1 = str1.replace("_table_name_",xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[0])
                str1 = str1.replace("_subject_area_",xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[1])

        if loop_num == 2:
            if xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[0].startswith("("):
                dummy = 1
            else:
                str1 = str1.replace("_brief_desc_",xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[0])

        if loop_num == 3:
            if xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[0].startswith("("):
                dummy = 1
            else:
                str1 = str1.replace("_sme_nm%d_"% loop3,xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[0])
                str1 = str1.replace("_sme_id%d_"% loop3,xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[1])
                loop3+=1
        
        if loop_num == 4:
            None
        
        if loop_num == 5:
            if xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[0].startswith("("):
                dummy = 1
            else:
                str1 = str1.replace("_load_freq%d_"% loop5,xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[0])
                str1 = str1.replace("_obj_type%d_"% loop5,xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[0])
                str1 = str1.replace("_env%d_"% loop5,xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[0])
                loop5+=1
        
        if loop_num == 6:
            if xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[0].startswith("("):
                dummy = 1
            else:
                str1 = str1.replace("_col%d_"% loop6,xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[0])
                str1 = str1.replace("_data_type%d_"% loop6,xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[1])
                str1 = str1.replace("_null%d_"% loop6,xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[2])
                str1 = str1.replace("_def%d_"% loop6,xl_sheet.row_values(row_idx,start_colx=0,end_colx=None)[3])
                loop6+=1
    elif xl_sheet.row_types(row_idx,start_colx=0,end_colx=None)[0] == 0:
        loop_num+=1
print (str1)
