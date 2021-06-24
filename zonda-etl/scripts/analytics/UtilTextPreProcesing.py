# coding=utf-8

#from nltk import word_tokenize
#from nltk import SnowballStemmer
import gensim
from gensim.utils import tokenize

class Documentos(object):
    
    def __init__(self, docs, stop_words = None):
        self.stop_words = stop_words
        self.docs = self.basic_preproc(docs)

    
      
    def reemplaza_tildes(self,word):
        word = word.replace('á','a')
        word = word.replace('é','e')
        word = word.replace('í','i')
        word = word.replace('ó','o')
        word = word.replace('ú','u')
        return word

    def basic_preproc(self,docs):
        #inicializa tamaño de corpus
        size_cor = len(docs)

        text_data = []
        for (texto,i) in zip(docs,range(0,size_cor)):
            t = self.preproc_sentence(texto)
            text_data.append(t)

        return text_data

    def preproc_sentence(self,doc):
        doc = self.reemplaza_tildes(doc)
        doc = tokenize(doc)
        doc = [t.lower() for t in doc if t.isalpha()]
        if self.stop_words != None:
            doc = [t for t in doc if t not in self.stop_words]
        return doc
    
    def stemming_docs(self, inplace = True):
        stemmer = SnowballStemmer('spanish')
        stemming_docs = []
        for sent in self.docs:
            stemming_docs.append([stemmer.stem(t) for t in sent])
        if inplace == True:
            self.docs = stemming_docs
        else:
            return stemming_docs
    
    def make_n_gram(self, min_count = 5, threshold = 15, inplace = True):
          
        # Inicializar modelos bi y tri grams
        bigram = gensim.models.Phrases(self.docs, min_count=min_count, threshold=threshold) # a thresholds más grandes menos frases.
        trigram = gensim.models.Phrases(bigram[self.docs], threshold=threshold)  

        bigram_mod = gensim.models.phrases.Phraser(bigram)
        trigram_mod = gensim.models.phrases.Phraser(trigram)
        if inplace == True:
            self.docs = list(trigram_mod[bigram_mod[self.docs]])
        else:
            return list(trigram_mod[bigram_mod[self.docs]])
       


#ToDO GRAFICOS

