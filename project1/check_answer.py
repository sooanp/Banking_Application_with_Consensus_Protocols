
client_phonebook = {"A": 0, "B": 1, "C": 2, "D": 3, "E": 4,
                    "F": 5, "G": 6, "H": 7, "I": 8, "J": 9}
client_balance = {"A": 100, "B": 100, "C": 100, "D": 100, "E": 100,
                  "F": 100, "G": 100, "H": 100, "I": 100, "J": 100}

from input_parser import parse_csv_file

def execute_transaction(client_balance, transaction):
    sender, receiver, amt = transaction
    if client_balance[sender] >= amt:
        client_balance[sender] -= amt
        client_balance[receiver] += amt
    return client_balance


all_transactions = parse_csv_file()
for transaction in all_transactions:
    if len(transaction) == 3:
        reformatted = (transaction[0], transaction[1], int(transaction[2]))
        client_balance = execute_transaction(client_balance, reformatted)
print(client_balance)
