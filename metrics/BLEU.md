# BLEU (Bilingual Evaluation Understudy Score)

## Описание

BLEU - метрика используемая в обработке естественного языка (NLP) и машинном переводе, 
измеряет, насколько сгенерированный текст совпадает с эталонным переводом 
по n-граммам (последовательностей из n последовательных слов).
Он вычисляет точность, учитывая, сколько n-грамм в сгенерированном тексте соответствует таковому в справочном тексте(ах). 
Затем показатель точности модифицируется штрафом за краткость, чтобы избежать предпочтения более коротких переводов.
Метрика BLEU учитывает точность для разных n-грамм (1-граммы, 2-граммы, 3-граммы, 4-граммы), 
и объединяет их через взвешенное среднее (обычно равновесное). 
Это позволяет учитывать не только отдельные слова, но и более длинные последовательности.
Значение метрики лежит в диапазоне [ 0,1 ] или [ 0 % , 100 % ]. Чем выше BLEU, тем лучше.
Метрика, разработана в IBM [Papineni et al.](https://aclanthology.org/P02-1040.pdf) в 2001.

## пример расчета метрики

```Python
from nltk.translate.bleu_score import corpus_bleu, SmoothingFunction

# Пример эталонов и кандидатов
references = [
    [['The', 'cat', 'is', 'on', 'the', 'mat'],
     ['There', 'is', 'a', 'cat', 'on', 'the', 'mat']],
    [['The', 'dog', 'sat', 'under', 'the', 'tree'],
     ['A', 'dog', 'was', 'sitting', 'beneath', 'the', 'tree']]
]

candidates = [
    ['The', 'cat', 'is', 'on', 'the', 'mat'],
    ['The', 'dog', 'is', 'under', 'the', 'tree']
]

# Расчёт BLEU для всей модели
smoothie = SmoothingFunction().method1
score = corpus_bleu(references, candidates, smoothing_function=smoothie)

print(f"BLEU Score for the model: {score:.4f}")

```

## Границы применения

### Не учитывает смысл и порядок слов глобально

BLEU сравнивает только точные n-граммы. Если слова переставлены, но смысл сохранился — BLEU даст низкий результат.

METEOR, BERTScore (учитывают порядок слов и семантическую близость).

###  Чувствительность к пунктуации

BLEU учитывает пунктуацию как отдельные токены, что иногда неправильно.

METEOR (встроенная нормализация).

### Нечувствительность к синонимам и парафразам

BLEU оценивает совпадение только на основе точных совпадений слов.

METEOR, BERTScore (используют семантическую близость).

### Не учитывает полноту (Recall)

BLEU измеряет только точность (Precision - сколько n-грамм из сгенерированного текста совпадают с эталоном), 
но не полноту (Recall - сколько n-грамм из эталона модель правильно сгенерировала.).

METEOR (учитывает и Precision, и Recall).

### Плохо работает на уровне предложений

BLEU надёжен при оценке больших корпусов, но нестабилен для отдельных предложений.

ROUGE (особенно ROUGE-L) и BERTScore.

## Пример несколько метрик

```Python
import nltk
import torch
import numpy as np
from nltk.translate.bleu_score import sentence_bleu, SmoothingFunction
from nltk.translate.meteor_score import meteor_score
from rouge_score import rouge_scorer
from transformers import BertTokenizer, BertModel
from sklearn.metrics.pairwise import cosine_similarity

nltk.download('wordnet')

# === Данные для анализа ===
references = [
    ["The cat is on the mat", "The cat lies on the mat", "The cat sits on the mat"],
    ["A man is eating food", "Someone is eating a meal", "A person is having lunch"],
    ["There is a dog in the garden", "A dog is in the backyard", "The dog is in the yard"]
]
hypotheses = [
    "The cat sat on the mat",
    "A person eats some food",
    "There is a dog in the yard"
]

# === Загрузка модели BERT ===
tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
model = BertModel.from_pretrained('bert-base-uncased')

# Функция для получения эмбеддингов BERT
def get_embeddings(text):
    inputs = tokenizer(text, return_tensors="pt")
    outputs = model(**inputs)
    embeddings = outputs.last_hidden_state.mean(dim=1)
    return embeddings.detach().numpy()

# Функции для расчёта метрик
def calculate_bleu(references, hypothesis):
    smoothie = SmoothingFunction().method4
    references_tokenized = [ref.split() for ref in references]
    return sentence_bleu(references_tokenized, hypothesis.split(), smoothing_function=smoothie)

def calculate_meteor(references, hypothesis):
    return np.mean([meteor_score([ref], hypothesis) for ref in references])

def calculate_rouge(references, hypothesis):
    scorer = rouge_scorer.RougeScorer(['rouge1', 'rougeL'], use_stemmer=True)
    rouge1_scores, rougeL_scores = [], []
    
    for ref in references:
        scores = scorer.score(ref, hypothesis)
        rouge1_scores.append(scores['rouge1'].fmeasure)
        rougeL_scores.append(scores['rougeL'].fmeasure)
        
    return np.mean(rouge1_scores), np.mean(rougeL_scores)

def calculate_bertscore(references, hypothesis):
    ref_embeddings = np.mean([get_embeddings(ref) for ref in references], axis=0)
    hyp_embedding = get_embeddings(hypothesis)
    return cosine_similarity(ref_embeddings, hyp_embedding)[0][0]

# === Рассчёт метрик для всех предложений ===
bleu_scores = []
meteor_scores = []
rouge1_scores = []
rougeL_scores = []
bertscore_scores = []

for ref_list, hyp in zip(references, hypotheses):
    bleu_scores.append(calculate_bleu(ref_list, hyp))
    meteor_scores.append(calculate_meteor(ref_list, hyp))
    rouge1, rougeL = calculate_rouge(ref_list, hyp)
    rouge1_scores.append(rouge1)
    rougeL_scores.append(rougeL)
    bertscore_scores.append(calculate_bertscore(ref_list, hyp))

# Усреднение значений по всем предложениям
avg_bleu = np.mean(bleu_scores)
avg_meteor = np.mean(meteor_scores)
avg_rouge1 = np.mean(rouge1_scores)
avg_rougeL = np.mean(rougeL_scores)
avg_bertscore = np.mean(bertscore_scores)

# === Создание объединённой метрики ===
alpha, beta, gamma, delta, epsilon = 0.2, 0.2, 0.2, 0.2, 0.2
combined_score = (alpha * avg_bleu + beta * avg_meteor + gamma * avg_rouge1 + 
                  delta * avg_rougeL + epsilon * avg_bertscore)

# === Вывод результатов ===
print(f"Средний BLEU Score: {avg_bleu:.4f}")
print(f"Средний METEOR Score: {avg_meteor:.4f}")
print(f"Средний ROUGE-1 Score: {avg_rouge1:.4f}")
print(f"Средний ROUGE-L Score: {avg_rougeL:.4f}")
print(f"Средний BERTScore: {avg_bertscore:.4f}")
print(f"\nОбъединённая метрика: {combined_score:.4f}")

```