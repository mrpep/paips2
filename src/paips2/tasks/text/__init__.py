from paips2.core import Task
import string
import nltk
from nltk.stem.porter import PorterStemmer
import numpy as np

class TextStandarizer(Task):
    def get_output_names(self):
        return ['text','tokens']

    def get_valid_parameters(self):
        return ['in'], ['lowercase','remove_punctuation','remove_stop_words','language','stem']

    def process(self):
        text = self.config['in']
        if self.config.get('lowercase',False):
            text = text.lower()
        if self.config.get('remove_punctuation',False):
            text = "".join([char for char in text if char not in string.punctuation])
        tokens = nltk.word_tokenize(text)
        if self.config.get('remove_stop_words'):
            if not hasattr(self, 'stop_words'):
                from nltk.corpus import stopwords
                self.stop_words = stopwords.words(self.config.get('language','english'))
            
            tokens = [t for t in tokens if t not in self.stop_words]
        if self.config.get('stem'):
            porter = PorterStemmer()
            tokens = [porter.stem(word) for word in tokens] #Stemizar
        
        return ' '.join(tokens), tokens
        
class WordOneHotVector(Task):
    def get_valid_parameters(self):
        return ['in'], ['special_tokens', 'input_type', 'column_in', 'column_out', 'max_seq_len']

    def get_output_names(self):
        return ['out','word_to_index','index_to_word']

    def process(self):
        text = self.config.get('in')
        input_type = self.config.get('input_type','dataframe')
        special_tokens = set(self.config.get('special_tokens',[]))
        max_seq_len = self.config.get('max_seq_len')
        for t in special_tokens:
            if t not in ['[SOS]','[EOS]','[PAD]',['SEP']]:
                raise Exception('Unrecognized special token')
        special_tokens = {t: i for i,t in enumerate(special_tokens)}
        if input_type == 'dataframe':
            column_in = self.config.get('column_in')
            column_out = self.config.get('column_out')
            all_words = np.concatenate(text[column_in].values)
            unique_words = np.unique(all_words)
            vocab = {w: i+len(special_tokens) for i,w in enumerate(unique_words)}
            vocab.update(special_tokens)

            def vectorize_fn(x):
                v = []
                if '[SOS]' in special_tokens:
                    v.append(vocab['[SOS]'])
                for w in x:
                    v.append(vocab[w])
                if '[EOS]' in special_tokens:
                    v.append(vocab['[EOS]'])
                if max_seq_len is not None:
                    if len(v) < max_seq_len:
                        v.extend([vocab['[PAD]']]*(max_seq_len - len(v)))
                    elif len(v) > max_seq_len:
                        v = v[:max_seq_len]
                    else:
                        pass
                return v

            text[column_out] = text[column_in].apply(vectorize_fn)
            index_to_word = {v:k for k,v in vocab.items()}

        return text, vocab, index_to_word