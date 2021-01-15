import csv
import os
import glob
import sys
import sqlparse
from sqlparse.sql import Where, Comparison, Parenthesis, Identifier

# Read CSV file wise

path = 'files'
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
print(query[query.index('from') + 1])

#print(col_names)


# Fetch Results

res = []
aggregate_functions = ['max','min','avg','count','sum']
keywords = ['where','group by','order by']
operators  = ['=','>','<','>=','<=']
if len(tables) == 1:                        # Single table
    #print(col_names)
    try:
        index_of_table = files.index(tables[0].lower()+'.csv')
    except:
        print("table doesn't exist")
        sys.exit()
    
    if col_names == '*':                    # All columns
        res = file_data[index_of_table]

    else:
        if 'distinct' in col_names:
            col_names = col_names.replace('distinct ','')
            col_names = col_names.split(',')
            cols = [i-1 for i in range(len(table_columns[index_of_table])) if table_columns[index_of_table][i].lower() in col_names]
            if len(cols) == 0:
                print("Column not found")
                sys.exit()
            
            for row in file_data[index_of_table]:
                temp_row = [row[i] for i in cols]
                res.append(temp_row)

            res = [list(t) for t in set(tuple(element) for element in res)]         # Remove duplicates

        elif any(x in col_names for x in aggregate_functions):              # Contains aggregate functions
            # TODO Handle * in this case

            if 'max' in col_names:
                col_names = col_names.replace('max(','')
                col_names = col_names.replace(')','')
                col_names = col_names.split(',')
                cols = [i-1 for i in range(len(table_columns[index_of_table])) if table_columns[index_of_table][i].lower() in col_names]
                
                if len(cols) == 0:
                    print("Column not found")
                    sys.exit()
                
                max_col = -100000
                for row in file_data[index_of_table]:
                    temp_row = [row[i] for i in cols]
                    if int(temp_row[0]) > max_col:
                        max_col = int(temp_row[0])
                    
                res.append([max_col])

            elif 'min' in col_names:
                col_names = col_names.replace('min(','')
                col_names = col_names.replace(')','')
                col_names = col_names.split(',')
                cols = [i-1 for i in range(len(table_columns[index_of_table])) if table_columns[index_of_table][i].lower() in col_names]

                if len(cols) == 0:
                    print("Column not found")
                    sys.exit()
                
                min_col = 1000000
                for row in file_data[index_of_table]:
                    temp_row = [row[i] for i in cols]
                    if int(temp_row[0]) < min_col:
                        min_col = int(temp_row[0])

                res.append([min_col])

            elif 'sum' in col_names:
                col_names = col_names.replace('sum(','')
                col_names = col_names.replace(')','')
                col_names = col_names.split(',')
                cols = [i-1 for i in range(len(table_columns[index_of_table])) if table_columns[index_of_table][i].lower() in col_names]

                if len(cols) == 0:
                    print("Column not found")
                    sys.exit()
                
                sum_col = 0
                for row in file_data[index_of_table]:
                    temp_row = [row[i] for i in cols]
                    sum_col += int(temp_row[0])

                res.append([sum_col])

            elif 'count' in col_names:
                col_names = col_names.replace('count(','')
                col_names = col_names.replace(')','')
                col_names = col_names.split(',')
                cols = [i-1 for i in range(len(table_columns[index_of_table])) if table_columns[index_of_table][i].lower() in col_names]

                if len(cols) == 0:
                    print("Column not found")
                    sys.exit()
                
                count = 0
                for row in file_data[index_of_table]:
                    temp_row = [row[i] for i in cols]
                    count += 1

                res.append([count])

            elif 'avg' in col_names:
                col_names = col_names.replace('avg(','')
                col_names = col_names.replace(')','')
                col_names = col_names.split(',')
                cols = [i-1 for i in range(len(table_columns[index_of_table])) if table_columns[index_of_table][i].lower() in col_names]

                if len(cols) == 0:
                    print("Column not found")
                    sys.exit()
                
                sum_col = 0
                count = 0
                for row in file_data[index_of_table]:
                    temp_row = [row[i] for i in cols]
                    sum_col += int(temp_row[0])
                    count += 1

                res.append([sum_col/count])


        else:
            col_names = col_names.split(',')
            cols = [i-1 for i in range(len(table_columns[index_of_table])) if table_columns[index_of_table][i].lower() in col_names]
            if len(cols) == 0:
                print("Column not found")
                sys.exit()
            
            for row in file_data[index_of_table]:
                temp_row = [row[i] for i in cols]
                res.append(temp_row)

    if 'where' in query:
        condition = ""
        start = query.index('where')+1

        while start < len(query) and query[start] not in keywords:
            condition += query[start]
            start += 1

        if condition[-1] == ';':
            condition = condition[:-1]

        # Without AND, OR
        operator = [o for o in operators if o in condition][0]
        cond_table = condition.split(operator)[0]
        val = int(condition.split(operator)[1])

        cols = [i-1 for i in range(len(table_columns[index_of_table])) if table_columns[index_of_table][i].lower() == cond_table][0]

        for x in res:
            if x[cols] != val:
                res.remove(x)

            

print(res)