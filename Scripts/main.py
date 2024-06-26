import sys
import csv
maxInt = sys.maxsize

while True:
    # decrease the maxInt value by factor 10 
    # as long as the OverflowError occurs.
    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt/10)

# Replace all "[" to "]" in original file and write to another file
with open('data.csv') as file:
    data = file.read().replace("[", "]")
with open('data_new.csv', 'w') as file:
    file.write(data)

# Read from that file and transform rows to a standard format (writing to another file)
with open('data_new.csv') as file: 
    reader = csv.reader(file, quotechar="]")
    
    with open('data_final.csv', 'w', newline='') as csvfile: 
        writer = csv.writer(csvfile)
        for i, row in enumerate(reader):
            for j, col in enumerate(row):
                row[j] = col.replace('"', "").replace(" ", "").replace(",","|")
            
            writer.writerow(row)