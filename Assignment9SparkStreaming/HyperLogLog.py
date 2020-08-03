from datasketch import HyperLogLog

data = [1, 2, 3, 5, 2, 4, 1, 6, 7]

hll = HyperLogLog()
for element in data:
    hll.update(str(element).encode('utf8'))

print(f'estimate: {hll.count()}')
print(f'real: {len(set(data))}')