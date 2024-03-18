import matplotlib.pyplot as plt

puts_times = []
gets_times = []

with open('output.txt', 'r') as file:
    lines = file.readlines()
    for line in lines:
        if 'puts' in line:
            time = int(line.split(':')[1].strip().split()[0])
            puts_times.append(time)
        elif 'gets' in line:
            time = int(line.split(':')[1].strip().split()[0])
            gets_times.append(time)

x_values = [1, 5, 20, 50, 100]

plt.plot(x_values, puts_times, label='Puts', marker='o')
plt.plot(x_values, gets_times, label='Gets', marker='o')

plt.title('Time taken for 1000 puts and gets')
plt.xlabel('num servers')
plt.ylabel('Time (seconds)')
plt.xticks(x_values)

plt.legend()

plt.grid(True)

plt.savefig('experiment1.png', format='png')
