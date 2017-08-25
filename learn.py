import csv
import urllib
import wget
import time 
from queue import Queue
from threading import Thread
from functools import partial
import multiprocessing
from multiprocessing.pool import Pool

# GLOBALLOCK = multiprocessing.Lock()

NUM_OF_PICS = 100

def download(link):
	try:
		wget.download(link)

	except TimeoutError:
		print("Timeout error, sleeping for 10 seconds")
		time.sleep(10)
		print("Waked up, going to try again")
		download(link)

	except FileNotFoundError as e:
		print(e, "skipping to next pic")

	except Exception as er:
		print(er, "going to sleep for 10 sec and skip")
		time(sleep(10))

def download_process(link):
	# GLOBALLOCK.acquire()
	try:
		wget.download(link)
	except Exception as e:
		print(e)
	# GLOBALLOCK.release()

class DownloadWorker(Thread):

	def __init__(self, queue):
		Thread.__init__(self)
		self.queue = queue
	# ÷
	def run(self):
		while True:
		# Get the work from the queue and expand the tuple
			link = self.queue.get()
			download(link)
			self.queue.task_done()

def one_thread(spamreader):
	sum = 0

	begin = time.time()

	queue = Queue()

	for x in range(20):
		worker = DownloadWorker(queue)
		# Setting daemon to True will let the main thread exit even though the workers are blocking
		worker.daemon = True
		worker.start()

	

	for row in spamreader:

		print("\n", sum + 1) #, "\n", row[1])

		link = row[1]

		# Option 1 - regular one-thread one-process
		# 0.48-0.68 spp (seconds per picture)

		download(link)

		sum += 1

		if sum > NUM_OF_PICS:
			break

	queue.join()

	time_elapsed = time.time() - begin

	print( "\nTime for mono: ", time_elapsed, 1.0 / (time_elapsed / NUM_OF_PICS), " pps")

def multi_thread(spamreader):

	sum = 0

	begin = time.time()

	queue = Queue()

	for x in range(20):
		worker = DownloadWorker(queue)
		# Setting daemon to True will let the main thread exit even though the workers are blocking
		worker.daemon = True
		worker.start()

	

	for row in spamreader:

		print("\n", sum + 1) #, "\n", row[1])

		link = row[1]

		# Option 2 - 20 threads one-process
		# 0.09-0.12 spp
		queue.put(link)

		sum += 1

		if sum > NUM_OF_PICS:
			break

	queue.join()

	time_elapsed = time.time() - begin

	print( "\nTime for multithreading: ", time_elapsed, 1.0 / (time_elapsed / NUM_OF_PICS), " pps")

def open_file(runner):
	with open('/Users/stivens/Developing/learning/prepared.csv', newline='') as csvfile:
		spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')

		runner(spamreader)
		

def multiproc(spamreader):

	links = []
	sum = 0

	begin = time.time()

	for row in spamreader:
		sum += 1
		links.append(row[1])
		if sum > NUM_OF_PICS:
			break

	p = Pool(8)

	x = p.map(download_process, links)

	p.close()
	p.join()

	time_elapsed = time.time() - begin

	print( "\nTime for multiprocessing: ", time_elapsed, 1.0 / (time_elapsed / NUM_OF_PICS), " pps")

def main():
	open_file(multiproc)
	# open_file()

if __name__ == '__main__':
    main()