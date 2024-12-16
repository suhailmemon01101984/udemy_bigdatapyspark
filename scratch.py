import re

text = "Hello World 123 Suhail"

# Match word characters
words = re.compile(r'\w+').split(text.lower())
print(words) 

# Match non-word characters
non_words = re.compile(r'\W+').split(text.lower())
print(non_words)
