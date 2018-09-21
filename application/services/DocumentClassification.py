import nltk, unicodedata, sys, os, string, gensim
import pandas as pd
import numpy as np
import tensorflow as tf

from nltk.stem.lancaster import LancasterStemmer
from tqdm import tqdm
from random import shuffle, random
from keras.models import Sequential, load_model
from keras.layers import *
from keras.optimizers import *


class LabelDoc():
    
    def __init__(self, doc_list, label, categories):
        self.label = label
        self.doc_list = doc_list
        self.categories = categories
        if 'C:\\' in cwd:
            self.dataURL = '%s\\data\\sets\\' % cwd
            self.lakeURL = 'C:\\Users\\d063195\\Desktop\\Lake\\'
            self.testURL = 'C:\\Users\\d063195\\Desktop\\Lake\\_TEST_\\'
            self.sep     = '\\'
        else:
            dataURL = '%s/data/sets' % cwd
            lakeURL = 'C:\\'  
            self.sep = '/'        
        
    def __iter__(self):
        # Document and tag generator
        for idx, doc in enumerate(self.doc_list):
            yield gensim.models.doc2vec.LabeledSentence(doc, [self.categories[self.label[idx][0]]])
            
    def train_data_with_label(self):
        train_text = []
        tr_words = []
        for cls in self.classes:
            train_data_path = self.lakeURL + cls
            for i in tqdm(os.listdir(train_data_path)):
                if i != '.DS_Store':
                    path = os.path.join(train_data_path, i)
                    df = pd.read_csv(path, delimiter='\n', error_bad_lines=False, engine='python')
                    dfnp = df.values
                    fa = dfnp.flatten('F')
                    try:
                        li = ''.join(fa.tolist())
                    except:
                        li = ''
                    dlgs = self.remove_punctuation(li)
                    dlg_tkn = nltk.word_tokenize(dlgs)
                    tr_words.extend(dlg_tkn)
                    train_text.append((dlg_tkn, self.one_hot_classes(cls)))
        
        return np.array(train_text)
    
    def remove_punctuation(self, text):
        return text.translate(self.tbl)    
    
    def get_tokenwords_for_new_data(self, path):
        test_text = []
        df = pd.read_csv(path, delimiter='\n')
        dfnp = df.values
        fa = dfnp.flatten('F')
        li = ''.join(fa.tolist())
        dlg = self.remove_punctuation(li)
        dlg_tkn = nltk.word_tokenize(dlg)
        test_text.append(dlg_tkn)
        return test_text

        
class SetData():
    
    def __init__(self):
        self.num_classes = 0
        self.classes = []
        self.train_data = None
        self.test_data = None
        self.stemmer = LancasterStemmer()
        self.tbl = str.maketrans({key: None for key in string.punctuation})
        cwd = os.getcwd()
        if 'C:\\' in cwd:
            self.dataURL = '%s\\data\\sets\\' % cwd
            self.lakeURL = 'C:\\Users\\d063195\\Desktop\\Lake\\'
            self.testURL = 'C:\\Users\\d063195\\Desktop\\Lake\\_TEST_\\'
            self.sep     = '\\'
        else:
            dataURL = '%s/data/sets' % cwd
            lakeURL = 'C:\\'  
            self.sep = '/'
            
    def one_hot_classes(self, clsname):
        ohenc = []
        
        for i in self.classes:
            if i == clsname:
                ohenc.append(1)
            else:
                ohenc.append(0)
        
        return ohenc
    
    def set_test_data(self):
        
        TestToTrainRatio = .3
        
        for folder in os.listdir(self.lakeURL):
            if folder != '_TEST_':
                for i in os.listdir(self.lakeURL + folder):
                    if random() < TestToTrainRatio:
                        try:
                            os.rename(self.lakeURL + folder + self.sep + i, self.testURL + '%s_%s' % (folder, i))
                        except:
                            # The file exists already and is a duplicate from the same or another folder in the Lake
                            os.remove(self.lakeURL + folder + self.sep + i)
    
    def set_classes(self):
        for folder in os.listdir(self.lakeURL):
            if folder != '_TEST_':
                self.classes.append(folder)
        self.num_classes = len(self.classes)
                
    def remove_punctuation(self, text):
        return text.translate(self.tbl)
    
    def train_data_with_label(self):
        train_text = []
        tr_words = []
        for cls in self.classes:
            train_data_path = self.lakeURL + cls
            for i in tqdm(os.listdir(train_data_path)):
                if i != '.DS_Store':
                    path = os.path.join(train_data_path, i)
                    df = pd.read_csv(path, delimiter='\n', error_bad_lines=False, engine='python')
                    dfnp = df.values
                    fa = dfnp.flatten('F')
                    try:
                        li = ''.join(fa.tolist())
                    except:
                        li = ''
                    dlgs = self.remove_punctuation(li)
                    dlg_tkn = nltk.word_tokenize(dlgs)
                    tr_words.extend(dlg_tkn)
                    train_text.append((dlg_tkn, self.one_hot_classes(cls)))
        tr_words = [self.stemmer.stem(w.lower()) for w in tr_words]
        tr_words = sorted(list(set(tr_words)))
        shuffle(train_text)
        return train_text, tr_words
    
    def test_data_with_label(self):
        test_text = []
        tst_words = []
        for cls in self.classes:
            for i in tqdm(os.listdir(self.test_data)):
                if i != '.DS_Store':
                    path = os.path.join(self.testURL, i)
                    df = pd.read_csv(path, delimiter='\n')
                    dfnp = df.values
                    fa = dfnp.flatten('F')
                    li = ''.join(fa.tolist())
                    dlgs = self.remove_punctuation(li)
                    dlg_tkn = nltk.word_tokenize(dlgs)
                    tst_words.extend(dlg_tkn)
                    test_text.append((dlg_tkn, self.one_hot_classes(cls)))
        tst_words = [self.stemmer.stem(w.lower()) for w in tst_words]
        tst_words = sorted(list(set(tst_words)))
        return test_text, tst_words
    
    def bag_of_words(self, data, wrds):
        model_data = []
        for i in data:
            bow = []
            tokenized = i[0]
            tokenized = [self.stemmer.stem(word.lower()) for word in tokenized]
            for w in wrds:
                if w in tokenized:
                    bow.append(1) 
                else:
                    bow.append(0)
            model_data.append([bow, i[1]])
        return np.array(model_data)
    
    
            
        
SD = SetData()
SD.set_classes()
#SD.set_test_data()
train_data, tr_words = SD.train_data_with_label()
test_data, tst_words = SD.test_data_with_label()

training = SD.bag_of_words(train_data, tr_words)
training_data = list(training[:, 0])
training_label = list(training[:, 1])

testing = SD.bag_of_words(test_data, tr_words, 'test')
testing_data = list(testing[:, 0])
testing_label = list(testing[:, 1])

tc_model = Sequential()
tc_model.add(InputLayer(input_shape=[len(training_data[0])]))
tc_model.add(Dense(128, activation='relu'))
tc_model.add(Dense(128, activation='relu'))
tc_model.add(Dense(64, activation='relu'))
tc_model.add(Dense(32, activation='relu'))
tc_model.add(Dropout(rate=0.4))
tc_model.add(Dense(SD.num_classes, activation='softmax'))
optimizer = Adam(lr=1e-3)
tc_model.compile(optimizer=optimizer, loss='categorical_crossentropy', metrics=['accuracy'])
tc_model.fit(x=np.array(training_data), y=np.array(training_label), epochs=100, batch_size=25)
tc_model.summary()

tc_model.save('%smodel_tc.h5' % SD.dataURL)

