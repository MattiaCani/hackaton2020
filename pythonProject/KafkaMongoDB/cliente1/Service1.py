import tkinter as tk
import requests
import subprocess
import time

class StockAnalysisApp:
    def __init__(self, root):
        self.root = root
        self.root.title("ACME Stock Analysis")

        # Creazione dei bottoni per le varie analisi
        self.btn_gainers = tk.Button(self.root, text="Azioni guadagnate negli ultimi 2 minuti",
                                     command=self.get_gainers)
        self.btn_gainers.pack(pady=10)

        self.btn_losers = tk.Button(self.root, text="Azioni perse negli ultimi 10 minuti", command=self.get_losers)
        self.btn_losers.pack(pady=10)

        self.btn_statistics = tk.Button(self.root, text="Media e deviazione standard", command=self.get_statistics)
        self.btn_statistics.pack(pady=10)

        self.btn_count_last_10_seconds = tk.Button(self.root, text="Conteggio valori negli ultimi 10 secondi",
                                                   command=self.count_last_10_seconds)
        self.btn_count_last_10_seconds.pack(pady=10)

        # Etichetta per visualizzare i risultati
        self.lbl_result = tk.Label(self.root, text="")
        self.lbl_result.pack(pady=20)

        # Text widget per visualizzare i dati
        self.text_output = tk.Text(self.root, height=10, width=60)
        self.text_output.pack(pady=20)

        # Bottone ESCI
        self.btn_exit = tk.Button(self.root, text="ESCI", command=self.exit_app)
        self.btn_exit.pack(pady=10)

    def start_main_script(self):
        try:
            subprocess.Popen(['python', 'main1.py'])
            self.lbl_result.config(text="Avviato lo script principale.")
            # Attendi che il server Flask sia pronto
            time.sleep(5)
        except Exception as e:
            self.lbl_result.config(text=f"Errore durante l'avvio dello script: {str(e)}")

    def get_gainers(self):
        response = requests.get('http://localhost:5000/api/gainers')
        if response.status_code == 200:
            data = response.json()
            self.display_results(data)
        else:
            self.lbl_result.config(text="Errore durante il recupero dei dati")

    def get_losers(self):
        response = requests.get('http://localhost:5000/api/losers')
        if response.status_code == 200:
            data = response.json()
            self.display_results(data)
        else:
            self.lbl_result.config(text="Errore durante il recupero dei dati")

    def get_statistics(self):
        company = "Apple"  # Sostituire con l'input dell'utente
        response = requests.get(f'http://localhost:5000/api/statistics/{company}')
        if response.status_code == 200:
            data = response.json()
            self.display_results(data)
        else:
            self.lbl_result.config(text="Errore durante il recupero dei dati")

    def count_last_10_seconds(self):
        company = "Apple"  # Sostituire con l'input dell'utente
        response = requests.get(f'http://localhost:5000/api/count_last_10_seconds/{company}')
        if response.status_code == 200:
            data = response.json()
            self.display_results(data)
        else:
            self.lbl_result.config(text="Errore durante il recupero dei dati")

    def display_results(self, data):
        if isinstance(data, list):
            results = "\n".join([f"{item['company']}: {item['change_percent']}%" for item in data])
        elif isinstance(data, dict):
            results = "\n".join([f"{key}: {value}" for key, value in data.items()])
        else:
            results = "Nessun dato disponibile"

        # Pulisce il widget Text prima di aggiungere i nuovi risultati
        self.text_output.delete(1.0, tk.END)
        self.text_output.insert(tk.END, results)

        # Ridimensiona automaticamente la finestra in base al contenuto
        self.root.update_idletasks()
        self.root.geometry(f"{self.root.winfo_reqwidth()}x{self.root.winfo_reqheight()}")

    def exit_app(self):
        self.root.destroy()

if __name__ == "__main__":
    root = tk.Tk()
    app = StockAnalysisApp(root)
    app.start_main_script()  # Avvia lo script principale al lancio dell'app
    root.mainloop()
