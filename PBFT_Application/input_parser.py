import os
import csv

def parse_csv_file():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(script_dir, 'CSE535-F25-Project-2-Testcases.csv')

    sequence_set = []
    seq_num = None
    nodes = None
    failure_node = []
    failure_type = []
    all_transactions = []
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            if row[0] != '' and row[2] != '':
                if seq_num is not None:
                    sequence = (seq_num, all_transactions, nodes, failure_node, failure_type)
                    sequence_set.append(sequence)
                    all_transactions = []
                seq_num = row[0]
                nodes = [x.strip() for x in row[2].strip("[]").split(",")] if row[2] else []
                failure_node = [x.strip() for x in row[3].strip("[]").split(",")] if row[3] else []
                if row[4]:
                    failure_type = [x.strip() for x in row[4].strip("[]").split(";")]
                else:
                    failure_type = []
            transaction = [x.strip() for x in row[1].strip("()").split(",")]
            all_transactions.append(transaction)
        sequence = (seq_num, all_transactions, nodes, failure_node, failure_type)
        sequence_set.append(sequence)

    return sequence_set

if __name__ == "__main__":
    sequences = parse_csv_file()
    for seq in sequences:
        print(seq)

    k = ['dark(n1, n2)']
    for j in k:
        if "n1" in j:
            print("yes")