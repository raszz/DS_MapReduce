import random
import string

class FileGenerator:
    def __init__(self, split, N, alphabet, min_size, max_size):
        self.split = split
        self.N = N
        self.alphabet = alphabet
        self.min_size = min_size
        self.max_size = max_size

    def generate_words(self):
        words = []
        for _ in range(self.N):
            word = ''.join(random.choices(self.alphabet, k=random.randint(self.min_size, self.max_size)))
            words.append(word)
        return words

    def generate_and_split_file(self, filename):
        words = self.generate_words()
        chunk_size = len(words) // self.split
        if len(words) % self.split != 0:
            chunk_size += 1

        for i in range(self.split):
            chunk_start = i * chunk_size
            chunk_end = min((i + 1) * chunk_size, len(words))
            chunk_words = words[chunk_start:chunk_end]
            with open(f"{filename}_{i+1}.txt", 'w') as f:
                for word in chunk_words:
                    f.write(word)
                    f.write('\n')  # Adiciona uma quebra de linha após cada palavra



# Exemplo de uso:
split = 5
N = 100000
alphabet = ['a', 'b', 'c', 'd', 'e']  # Apenas as letras a, b, c, d, e  " alphabet = string.ascii_lowercase """  # Usa todas as letras minúsculas
min_size = 1
max_size = 10

file_generator = FileGenerator(split, N, alphabet, min_size, max_size)
for i in range(split):
    file_generator.generate_and_split_file(f"text_file")