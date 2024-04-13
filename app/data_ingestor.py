
import csv

class DataIngestor:
    def __init__(self, csv_path: str):
        data = []
        with open(csv_path, newline='') as csvfile:
            dictread = csv.DictReader(csvfile)
            for i in dictread:
                data.append(i) # Toate datele citite ajung in data
        self.data = data # Acestea sunt stocate in atributul self.data al obiectului curent
