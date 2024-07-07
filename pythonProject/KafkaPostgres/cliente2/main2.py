import threading
import time
from producer import start_producer
from consumer import start_consumer

if __name__ == "__main__":
    # Esempio di titoli selezionati dal cliente
    selected_companies = ["Apple", "Microsoft", "Amazon"]

    # Avvia il producer in un thread separato
    producer_thread = threading.Thread(target=start_producer, args=(selected_companies,))
    producer_thread.start()

    # Avvia il consumer nel thread principale
    start_consumer()

    # Attendi che il producer completi il suo lavoro
    producer_thread.join()

