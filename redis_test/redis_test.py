import concurrent.futures


def work(idx):
	print(idx)

with concurrent.futures.ThreadPoolExecutor(max_workers = 1000) as executor:
	for i in range(0, 100):
		executor.submit(work, i)