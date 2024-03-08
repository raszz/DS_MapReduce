from threading import Thread
from collections import defaultdict
import operator

class MapReduceController:
    def __init__(self, mapper, reducer):
        self.mapper = mapper
        self.reducer = reducer
        self.intermediate_results = defaultdict(list)

    def map_reduce(self, files):
        # Inicia uma thread para cada arquivo
        threads = []
        for file_name in files:
            t = Thread(target=self.map_file, args=(file_name,))
            threads.append(t)
            t.start()

        # Espera todas as threads terminarem
        for t in threads:
            t.join()

        # Salva os resultados intermediários em um arquivo temporário
        self.save_intermediate_results()

        # Inicia uma thread para cada chave e valores intermediários
        reduce_threads = []
        for word, counts in self.intermediate_results.items():
            t = Thread(target=self.reduce_wrapper, args=(word, counts))
            reduce_threads.append(t)
            t.start()

        # Espera todas as threads de redução terminarem
        for t in reduce_threads:
            t.join()

        # Organiza os resultados de acordo com a ordem de menos repetições para mais repetições
        sorted_results = dict(sorted(self.intermediate_results.items(), key=lambda x: x[1]))

        # Salva o resultado em um arquivo
        self.save_result(sorted_results)

    def map_file(self, file_name):
        # Realiza a função map no arquivo
        with open(file_name, 'r') as f:
            words = f.read().splitlines()
            for word in words:
                self.intermediate_results[word].append(1)

    def reduce_wrapper(self, word, counts):
        # Chama a função de redução para a chave e valores intermediários
        result = self.reducer(word, counts)
        # Salva o resultado na lista de resultados
        self.intermediate_results[word] = result

    def save_intermediate_results(self):
        # Salva os resultados intermediários em um único arquivo de texto
        with open("intermediate_results.txt", 'w') as f:
            for word, counts in self.intermediate_results.items():
                f.write(f"{word}: {counts}\n")

    def save_result(self, results):
        # Salva o resultado em um arquivo de texto
        with open("countingWords_result.txt", 'w') as f:
            for word, result in results.items():
                f.write(f"{word}: {result}\n")


# Função de redução para contar a frequência das palavras
def word_count_reducer(word, counts):
    return sum(counts)

# Exemplo de uso
files = ["text_file_1.txt", "text_file_2.txt", "text_file_3.txt", "text_file_4.txt", "text_file_5.txt"]
controller = MapReduceController(None, word_count_reducer)
controller.map_reduce(files)



