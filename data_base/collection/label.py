import re
from datetime import datetime
import pandas as pd
import spacy
import os
from pypdf import PdfReader

current_date = datetime.now().strftime("%Y-%m-%d")
result_dir = '../../result/arxiv'
pdf_dir = os.path.join(result_dir, 'pdf/')
result_file = os.path.join(result_dir, 'arxiv_2024-09-02_to_2024-09-09_labeled.csv')
log_file = open(os.path.join('../logs', f'arxiv_label_{current_date}.log'), 'a+')

key_words = ['GPU', 'TPU', 'NPU', 'MLU', 'NVIDIA', 'AMD', 'A100', 'H100', 'A800', 'H800', 'V100', 'MI100', 'MI200',
             'MI250', 'MI300', 'Graviton', 'Trainium', 'Inferentia', 'Gaudi', 'CUDA', 'ROCm']
key_affiliations = ['Google', 'Microsoft', 'Meta', 'OpenAI', 'Apple', 'Tesla']
key_words_set = set(key_words)
key_words_set_filtered = key_words_set - {'AMD', 'TPU', 'NPU', 'MLU'}
key_words_lower = [word.lower() for word in key_words]
key_words_lower_set = set(key_words_lower)

header = ['arxiv_code'] + key_words + key_affiliations
nlp = spacy.load("en_core_web_sm")


def get_affiliation(text, pagination):
    affiliations = {}
    if not pagination and len(text) > 200:
        text = text[:200]
    for affiliation in key_affiliations:
        if affiliation.lower() in text.lower():
            affiliations[affiliation] = 1
        else:
            affiliations[affiliation] = 0
    return affiliations


def count_key_words(text):
    doc = nlp(text)
    text_words = [token.text for token in doc]
    key_words_freq = {kword: 0 for kword in key_words_lower}

    for word in text_words:
        word_lower = word.lower()
        # 文本中的单词在关键词集合中(小写精确匹配)
        if word_lower in key_words_lower_set:
            key_words_freq[word_lower] += 1
        elif word_lower + 's' in key_words_lower_set:
            key_words_freq[word_lower] += 1
        # 文本中的单词包含关键词(区分大小写)
        else:
            for kword in key_words_set_filtered:
                if kword in word:
                    key_words_freq[kword.lower()] += 1
                    break
    return {kword: key_words_freq[kword.lower()] for kword in key_words}


def label(pdf_file):
    arxiv_code = pdf_file.rstrip('.pdf')
    reader = PdfReader(pdf_dir + pdf_file)

    all_text = ""
    for page in reader.pages[:-2]:
        page_text = page.extract_text()
        all_text += page_text
    key_words_freq = count_key_words(all_text)

    pattern = re.compile(r'(abstract|introduction)', re.IGNORECASE)
    first_page_text = reader.pages[0].extract_text()
    author_info_text = re.split(pattern, first_page_text, maxsplit=1)
    pagination = False
    if len(author_info_text) > 1:
        author_info_text = author_info_text[0]
        pagination = True
    else:
        print(f'{arxiv_code}.pdf exceeds.')
        author_info_text = first_page_text
    author_affiliation = get_affiliation(author_info_text, pagination)

    print(f'{arxiv_code}.pdf parsed.')
    return [arxiv_code] + list(key_words_freq.values()) + list(author_affiliation.values())


if __name__ == "__main__":
    pdf_files = os.listdir(pdf_dir)
    explored_files = set()
    if not os.path.exists(result_file) or os.path.getsize(result_file) == 0:
        pd.DataFrame(columns=header).to_csv(result_file, index=False, header=True)
    else:
        print('Result file already exists. New results will be appended.')
        log_file.seek(0)
        for line in log_file:
            explored_files.add(line.strip())

    for pdf_file_path in pdf_files:
        if pdf_file_path.endswith('.pdf'):
            if pdf_file_path in explored_files:
                print(f'{pdf_file_path} already explored.')
                continue
            try:
                labels = label(pdf_file_path)
                log_file.write(f'{pdf_file_path}\n')
            except Exception as e:
                print(f'{pdf_file_path} failed to parse.')
                print(e)
                continue
            pd.DataFrame([labels], columns=header).to_csv(result_file, index=False, header=False, mode='a+')
