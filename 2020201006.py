import csv
import os
import glob
import sys
import sqlparse
from sqlparse.sql import Where, Comparison, Parenthesis, Identifier

# Read CSV file wise

path = '.'
extension = 'csv'
os.chdir(path)
files = glob.glob('*.{}'.format(extension))
#print(files)

file_data = []                                    # 2D array representing Rows 
for file in files:
    with open(file, mode='r') as csv_file:
        reader = csv.reader(csv_file)
        data = list(reader)
        file_data.append(data)

#list1 = sorted(list1,key=lambda x: (x[1]))

#print(file_data)


# Insert Table details

# Attempt 1

# table_columns = []
# start = "<begin_table>"
# end = "<end_table>"
# buffer = ""
# log = False

# for line in open('metadata.txt'):
#   if line.startswith(start):
#     buffer = line
#     log = True
#   elif line.startswith(end):
#     buffer += line
#     log = False
#   elif log:
#     buffer += line

# print(buffer)


# Attempt 2

table_columns = []                              # Each index Represents list of col names
file1 = open('metadata.txt', 'r') 
Lines = file1.readlines() 
index = 0
table = False
temp_col = []
count = 0
# Strips the newline character 
for line in Lines:
    if line == "<begin_table>\n":
        table = True
    elif line == "<end_table>\n":
        table_columns.append(temp_col)
        temp_col = []
        index += 1
        table = False
    elif table:
        temp_col.append(line[:-1])
    
#print(table_columns)


# Query Processing

query = sys.argv[1]
#query = query.split(' ')


# Get column names to fetch

query = query.split(' ')
while "" in query:
    query.remove("")
query = [x.lower() for x in query]
start_word = "select"
end_word = "from"
start_index = query.index(start_word)
end_index = query.index(end_word)

col_names = " ".join(query[start_index+1:end_index])
if query[query.index('from') + 1][-1] == ';':
    tables = query[query.index('from') + 1][:-1].split(',')
else:
    tables = query[query.index('from') + 1].split(',')

#print(col_names)


# Fetch Results

res = []
aggregate_functions = ['max','min','avg','count','sum']
keywords = ['where','group','order']
operators  = ['=','>=','<=','>','<']
has_aggregate_func = False
if len(tables) == 1:                        # Single table
    #print(col_names)
    try:
        index_of_table = files.index(tables[0].lower()+'.csv')
    except:
        print("table doesn't exist")
        sys.exit()
    
    res = file_data[index_of_table]
    if 'where' in query:
        #res = file_data[index_of_table]
        condition = ""
        start = query.index('where')+1

        while start < len(query) and query[start] not in keywords:
            condition += query[start]
            start += 1

        if condition[-1] == ';':
            condition = condition[:-1]

        if 'and' in condition:
            conditions = condition.split('and')
            for condition in conditions:
                operator = [o for o in operators if o in condition][0]
                cond_table = condition.split(operator)[0]
                val = int(condition.split(operator)[1])

                try:
                    cols = [i-1 for i in range(len(table_columns[index_of_table])) if table_columns[index_of_table][i].lower() == cond_table][0]
                    cols_to_remove = []
                    for x in res:
                        if operator == '=':
                            if int(x[cols]) != val:
                                cols_to_remove.append(x)

                        elif operator == '<':
                            if int(x[cols]) >= val:
                                cols_to_remove.append(x)

                        elif operator == '>':
                            if int(x[cols]) <= val:
                                cols_to_remove.append(x)

                        elif operator == '>=':
                            if int(x[cols]) < val:
                                cols_to_remove.append(x)

                        elif operator == '<=':
                            if int(x[cols]) > val:
                                cols_to_remove.append(x)                
                    
                    for x in cols_to_remove: 
                        res.remove(x)
                except:
                    print("Col doesn't exist")
                    sys.exit()

        elif 'or' in condition:
            conditions = condition.split('or')
            
            condition = conditions[0]

            operator = [o for o in operators if o in condition][0]
            cond_table = condition.split(operator)[0]
            val = int(condition.split(operator)[1])

            try:
                cols = [i-1 for i in range(len(table_columns[index_of_table])) if table_columns[index_of_table][i].lower() == cond_table][0]
                cols_to_remove = []
                for x in res:
                    if operator == '=':
                        if int(x[cols]) != val:
                            cols_to_remove.append(x)

                    elif operator == '<':
                        if int(x[cols]) >= val:
                            cols_to_remove.append(x)

                    elif operator == '>':
                        if int(x[cols]) <= val:
                            cols_to_remove.append(x)

                    elif operator == '>=':
                        if int(x[cols]) < val:
                            cols_to_remove.append(x)

                    elif operator == '<=':
                        if int(x[cols]) > val:
                            cols_to_remove.append(x)
            except:
                print("Col doesn't exist")
                sys.exit()         
            
            condition = conditions[1]

            operator = [o for o in operators if o in condition][0]
            cond_table = condition.split(operator)[0]
            val = int(condition.split(operator)[1])

            try:
                cols = [i-1 for i in range(len(table_columns[index_of_table])) if table_columns[index_of_table][i].lower() == cond_table][0]
                temp_cols_to_remove = []
                for x in cols_to_remove:
                    if operator == '=':
                        if int(x[cols]) == val:
                            temp_cols_to_remove.append(x)

                    elif operator == '<':
                        if int(x[cols]) < val:
                            temp_cols_to_remove.append(x)

                    elif operator == '>':
                        if int(x[cols]) > val:
                            temp_cols_to_remove.append(x)

                    elif operator == '>=':
                        if int(x[cols]) >= val:
                            temp_cols_to_remove.append(x)

                    elif operator == '<=':
                        if int(x[cols]) <= val:
                            temp_cols_to_remove.append(x)
            except:
                print("Col doesn't exist")
                sys.exit()                    
            
            for x in temp_cols_to_remove:
                cols_to_remove.remove(x)        
            
            for x in cols_to_remove: 
                res.remove(x)
                
        
        # Without AND, OR
        else:
            operator = [o for o in operators if o in condition][0]
            cond_table = condition.split(operator)[0]
            val = int(condition.split(operator)[1])

            try:
                cols = [i-1 for i in range(len(table_columns[index_of_table])) if table_columns[index_of_table][i].lower() == cond_table][0]
                cols_to_remove = []
                for x in res:
                    if operator == '=':
                        if int(x[cols]) != val:
                            cols_to_remove.append(x)

                    elif operator == '<':
                        if int(x[cols]) >= val:
                            cols_to_remove.append(x)

                    elif operator == '>':
                        if int(x[cols]) <= val:
                            cols_to_remove.append(x)

                    elif operator == '>=':
                        if int(x[cols]) < val:
                            cols_to_remove.append(x)

                    elif operator == '<=':
                        if int(x[cols]) > val:
                            cols_to_remove.append(x)                
                
                for x in cols_to_remove: 
                    res.remove(x)
            except:
                print("Col doesn't exist")
                sys.exit()


    if 'distinct' in col_names:
        col_names = col_names.replace('distinct ','')
        col_names = col_names.split(',')
        col_names = [x.strip(' ') for x in col_names]

        cols = [i-1 for i in range(len(table_columns[index_of_table])) if table_columns[index_of_table][i].lower() in col_names]
        if len(cols) == 0:
            print("Column not found")
            sys.exit()
        
        # for row in file_data[index_of_table]:
        #     temp_row = [row[i] for i in cols]
        #     res.append(temp_row)
        for row in res:
            res[res.index(row)]= [j for i, j in enumerate(row) if i in cols]
            #row = temp_row

        res = [list(t) for t in set(tuple(element) for element in res)]         # Remove duplicates
        

    elif any(x in col_names for x in aggregate_functions):              # Contains aggregate functions
        # TODO Handle * in this case

        has_aggregate_func = True
        if 'max' in col_names:
            col_names = col_names.replace('max(','')
            col_names = col_names.replace(')','')
            col_names = col_names.split(',')
            col_names = [x.strip(' ') for x in col_names]
            cols = [i-1 for i in range(len(table_columns[index_of_table])) if table_columns[index_of_table][i].lower() in col_names]
            
            if len(cols) == 0:
                print("Column not found")
                sys.exit()
            
            max_col = -100000
            for row in res:
                temp_row = [row[i] for i in cols]
                if int(temp_row[0]) > max_col:
                    max_col = int(temp_row[0])
                
            res = []
            res.append([max_col])

        elif 'min' in col_names:
            col_names = col_names.replace('min(','')
            col_names = col_names.replace(')','')
            col_names = col_names.split(',')
            col_names = [x.strip(' ') for x in col_names]
            cols = [i-1 for i in range(len(table_columns[index_of_table])) if table_columns[index_of_table][i].lower() in col_names]

            if len(cols) == 0:
                print("Column not found")
                sys.exit()
            
            min_col = 1000000
            for row in res:
                temp_row = [row[i] for i in cols]
                if int(temp_row[0]) < min_col:
                    min_col = int(temp_row[0])

            res = []
            res.append([min_col])

        elif 'sum' in col_names:
            col_names = col_names.replace('sum(','')
            col_names = col_names.replace(')','')
            col_names = col_names.split(',')
            col_names = [x.strip(' ') for x in col_names]
            cols = [i-1 for i in range(len(table_columns[index_of_table])) if table_columns[index_of_table][i].lower() in col_names]

            if len(cols) == 0:
                print("Column not found")
                sys.exit()
            
            sum_col = 0
            for row in res:
                temp_row = [row[i] for i in cols]
                sum_col += int(temp_row[0])

            res = []
            res.append([sum_col])

        elif 'count' in col_names:
            col_names = col_names.replace('count(','')
            col_names = col_names.replace(')','')
            col_names = col_names.split(',')
            col_names = [x.strip(' ') for x in col_names]
            if col_names[0] == '*':
                total_rows = len(res)
                res = []
                res.append([total_rows])
            else:
                cols = [i-1 for i in range(len(table_columns[index_of_table])) if table_columns[index_of_table][i].lower() in col_names]

                if len(cols) == 0:
                    print("Column not found")
                    sys.exit()
                
                count = 0
                for row in res:
                    temp_row = [row[i] for i in cols]
                    count += 1

                res = []
                res.append([count])

        elif 'avg' in col_names:
            col_names = col_names.replace('avg(','')
            col_names = col_names.replace(')','')
            col_names = col_names.split(',')
            col_names = [x.strip(' ') for x in col_names]
            cols = [i-1 for i in range(len(table_columns[index_of_table])) if table_columns[index_of_table][i].lower() in col_names]

            if len(cols) == 0:
                print("Column not found")
                sys.exit()
            
            sum_col = 0
            count = 0
            for row in res:
                temp_row = [row[i] for i in cols]
                sum_col += int(temp_row[0])
                count += 1

            res = []
            res.append([sum_col/count])


    elif col_names != '*':
        col_names = col_names.split(',')
        col_names = [x.strip(' ') for x in col_names]
        cols = [i-1 for i in range(len(table_columns[index_of_table])) if table_columns[index_of_table][i].lower() in col_names]
        if len(cols) == 0:
            print("Column not found")
            sys.exit()
        
        for row in res:
            res[res.index(row)]= [j for i, j in enumerate(row) if i in cols]

    if 'order' in query:
        order_by_col = query[query.index('order')+2]
        col_index = table_columns[index_of_table].index(order_by_col.upper())-1
        try:
            asc_desc = query[query.index('order')+3]

            if asc_desc == 'desc':
                res = sorted(res,key=lambda x: (x[col_index]), reverse=True)
            else:
                res = sorted(res,key=lambda x: (x[col_index]))
        except:
            print("Invalid order")
            sys.exit()

# Display output

if has_aggregate_func:
    print("<{}.{}>".format(table_columns[index_of_table][0],col_names[0].upper()))
    print(res[0][0])
else:
    if col_names == '*':
        print("<", end=" ")    
        for i in range(1, len(table_columns[index_of_table])):
            if i == len(table_columns[index_of_table])-1:
                print("{}.{}".format(table_columns[index_of_table][0], table_columns[index_of_table][i].upper()), end=" ")
            else:
                print("{}.{}".format(table_columns[index_of_table][0], table_columns[index_of_table][i].upper()), end=", ")
        print(">")

    else:
        temp_cols = [s.lower() for s in table_columns[index_of_table]]
        col_names = sorted(col_names,key=temp_cols.index)

        print("<", end=" ")
        for x in col_names:
            if col_names.index(x) == len(col_names)-1:
                print("{}.{}".format(table_columns[index_of_table][0], x.upper()), end=" ")
            else:
                print("{}.{}".format(table_columns[index_of_table][0], x.upper()), end=", ")
        print(">")
    for x in res:
        for y in x:
            print(y, end=" ")
        print("")