import threading
from time import sleep
import numpy as np
import time

# nome della compagnia
companies = ["Apple", "Microsoft", "Samsung", "Philips", "Toyota", "BMW", "FCA", "Amazon", "IBM", "Dell"]
# prezzo iniziale
start_quote = [1032.03, 131.23, 435.1, 641.23, 121.53, 344.9, 313.6, 931.34, 1112.97, 394.1]

# thread
def worker(i):
    company = str(companies[i])
    quote = start_quote[i]
    while True:
        timestamp = round(time.time(),3)
        print(company + " " + str(timestamp) + " " + str(quote))
        # nuovo prezzo
        quote = round(np.random.normal(quote, 0.1),2)
        # attesa
        delay = np.random.normal(0.01, 0.1)
        sleep(abs(delay))
        return

threads = []
for i in range(10):
    t = threading.Thread(target=worker, args=(i,))
    threads.append(t)
    t.start()