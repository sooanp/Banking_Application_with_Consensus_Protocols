import commands


if __name__ == "__main__":
    for i in range(1,6):
        print(f"\n\n======ALL LOGS FOR NODE: {i} ================")
        print(commands.PrintLog(i))


    # Test Case 1
    # print(f"\n\n======STATUS OF ALL NODES ================")
    # for i in range(1,6):
    #     seq = commands.PrintStatus(i)
    #     print(f"seq {i}: ",seq)

    # Test Case 2
    # print(f"\n\n======STATUS OF ALL NODES ================")
    # for i in range(6,8):
    #     seq = commands.PrintStatus(i)
    #     print(f"seq {i}: ",seq)

    # Test Case 3
    # print(f"\n\n======STATUS OF ALL NODES ================")
    # for i in range(8,10):
    #     seq = commands.PrintStatus(i)
    #     print(f"seq {i}: ",seq)

    # Test Case 4
    # print(f"\n\n======STATUS OF ALL NODES ================")
    # for i in range(10,12):
    #     seq = commands.PrintStatus(i)
    #     print(f"seq {i}: ",seq)

    # Test Case 5
    # print(f"\n\n======STATUS OF ALL NODES ================")
    # for i in range(12,14):
    #     seq = commands.PrintStatus(i)
    #     print(f"seq {i}: ",seq)

    # Test Case 6
    # print(f"\n\n======STATUS OF ALL NODES ================")
    # for i in range(12,17):
    #     seq = commands.PrintStatus(i)
    #     print(f"seq {i}: ",seq)
    
    # Test Case 7
    # print(f"\n\n======STATUS OF ALL NODES ================")
    # for i in range(17,20):
    #     seq = commands.PrintStatus(i)
    #     print(f"seq {i}: ",seq)

    # Test Case 8
    # print(f"\n\n======STATUS OF ALL NODES ================")
    # for i in range(20,26):
    #     seq = commands.PrintStatus(i)
    #     print(f"seq {i}: ",seq)
    
    # Test Case 9
    # print(f"\n\n======STATUS OF ALL NODES ================")
    # for i in range(24,30):
    #     seq = commands.PrintStatus(i)
    #     print(f"seq {i}: ",seq)

    # Test Case 10
    print(f"\n\n======STATUS OF ALL NODES ================")
    for i in [30, 40, 50, 60, 70, 79]:
        seq = commands.PrintStatus(i)
        print(f"seq {i}: ",seq)

# print(commands.PrintStatus(seq))