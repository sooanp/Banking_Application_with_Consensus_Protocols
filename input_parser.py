import os
import csv



def parse_csv_file():

    script_dir = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(script_dir, 'sample_input.csv')

    sequence_set = []
    seq_num = None
    nodes = None
    all_transactions = []
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        i = 0
        for row in reader:
            if i == 0:
                i+=1
                continue

            if row[0] != '' and row[2] != '':
                if seq_num==None and nodes == None:
                    seq_num = row[0]
                    str_nodes = [x.strip() for x in row[2].strip("[]").split(",")]

                    nodes = str_nodes
                else:
                    sequence = (seq_num, all_transactions, nodes)
                    sequence_set.append(sequence)
                    all_transactions = []
                    seq_num = row[0]
                    str_nodes = [x.strip() for x in row[2].strip("[]").split(",")]
                    nodes = str_nodes
            transaction = [x.strip() for x in row[1].strip("()").split(",")]
            all_transactions.append(transaction)
        sequence = (seq_num, all_transactions, nodes)
        sequence_set.append(sequence)

    return sequence_set

