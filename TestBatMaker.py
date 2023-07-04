def neighbour_generator(neighbour_range):
    for i in neighbour_range:
        for j in neighbour_range:
            yield i, j


for i, j in neighbour_generator(range(-1, 2)):
    print(i,j)
    break
else:
    print("1")
for i, j in neighbour_generator(range(-1, 2)):
    print(i,j)
else:
    print("2")

for i in range(-1,2):
    for j in range(-1, 2):
        print(i,j)