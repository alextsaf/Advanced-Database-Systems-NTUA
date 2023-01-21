import pandas as pd

queries = ['Query1.csv','Query2.csv','Query3.csv','Query4.csv','Query5.csv',]
oneWorker = "/home/user/ADB-code/results/one-worker/"
twoWorkers = "/home/user/ADB-code/results/two-workers/"

for query in queries:
    print(query)
    csv = pd.read_csv(oneWorker + query)
    print(csv.to_markdown(index = False))

print("Query3RDD.csv")
csv = pd.read_csv(oneWorker + "Query3RDD.csv", names = ['Month', 'Half', 'AVG Trip Distance', 'AVG Amount'], header = None)
print(csv.to_markdown(index = False))



print("Times.csv")
times1 = pd.read_csv(oneWorker + "Times.csv", names = ['Query', 'Time (One)'], index_col = 0, header = None)
times2 = pd.read_csv(twoWorkers + "Times.csv", names = ['Query', 'Time (Two)'], index_col = 0, header = None)

times_whole = times1.join(times2).groupby('Query').mean()
print(times_whole.to_markdown())


