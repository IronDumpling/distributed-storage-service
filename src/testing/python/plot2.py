import matplotlib.pyplot as plt

puts_total = {}
gets_total = {}
client_counts = [1, 5, 20, 50, 100]

with open('output2.txt', 'r') as file:
    lines = file.readlines()

current_clients = 0
for line in lines:
    if 'num clients:' in line:
        current_clients = int(line.split(':')[1].strip())
        if current_clients not in puts_total:
            puts_total[current_clients] = 0
            gets_total[current_clients] = 0
    elif 'Time taken for puts:' in line:
        time = int(line.split(':')[1].strip().split()[0])
        puts_total[current_clients] += time
    elif 'Time taken for gets:' in line:
        time = int(line.split(':')[1].strip().split()[0])
        gets_total[current_clients] += time

puts_avg = [puts_total[client] / client for client in client_counts]
gets_avg = [gets_total[client] / client for client in client_counts]

plt.plot(client_counts, puts_avg, label='PUT Time', marker='o')
plt.plot(client_counts, gets_avg, label='GET Time', marker='o')

plt.title('Time taken for 1000 puts and gets')
plt.xlabel('Number of Clients')
plt.ylabel('Average Time (seconds)')
plt.xticks(client_counts)

plt.legend()
plt.grid(True)

plt.savefig('experiment2.png')