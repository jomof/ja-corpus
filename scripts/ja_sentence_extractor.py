import re
import logging

EMOJI_REGEX = re.compile(r"[\u2600-\u27BF\U0001F300-\U0001F6FF\U0001F900-\U0001F9FF]")

def count_kanji_kana(text):
    regex = r"[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF\uFF65-\uFF9F]"
    return len(re.findall(regex, text))

def starts_with_valid_char(text):
    if not text:
        return False
        
    # Strip leading emojis and spaces
    text = EMOJI_REGEX.sub("", text).strip()
    
    # Strip leading quote markers and brackets (including Japanese quotes)
    text = re.sub(r"^[＞>［\[＜<【｢「（(]+", "", text).strip()
    
    if not text:
        return False
        
    first_char = text[0]
    
    # Check Kanji/Kana
    if ('\u3040' <= first_char <= '\u309F' or
        '\u30A0' <= first_char <= '\u30FF' or
        '\u4E00' <= first_char <= '\u9FFF'):
        return True
        
    # Check Half-width Katakana
    if '\uFF65' <= first_char <= '\uFF9F':
        return True
        
    # Check Latin Letters (Half-width and Full-width)
    if ('A' <= first_char <= 'Z' or 'a' <= first_char <= 'z' or
        '\uFF21' <= first_char <= '\uFF3A' or '\uFF41' <= first_char <= '\uFF5A'):
        return True
        
    # Check Digits (Half-width and Full-width)
    if ('0' <= first_char <= '9' or '\uFF10' <= first_char <= '\uFF19'):
        return True
        
    return False

def contains_hiragana_kana(text):
    regex = r"[\u3040-\u309F\u30A0-\u30FF\uFF65-\uFF9F]"
    return bool(re.search(regex, text))

def is_valid_sentence(tokens):
    if not tokens:
        return False
        
    # Rule 1: Disallow ending with comma (absolute last token)
    last_abs_pos = tokens[-1].part_of_speech()
    if len(last_abs_pos) > 1 and "読点" in last_abs_pos[1]:
        return False
    if tokens[-1].surface() in ["、", ","]:
        return False
        
    # Find last non-symbol, non-blank token
    last_non_sym = None
    for t in reversed(tokens):
        pos = t.part_of_speech()
        if len(pos) > 0 and pos[0] not in ["補助記号", "空白"]:
            last_non_sym = t
            break
            
    if not last_non_sym:
        return False
        
    last_surface = last_non_sym.surface()
    last_pos = last_non_sym.part_of_speech()
    
    # Rule 2: Disallow ending with certain particles
    if len(last_pos) > 0 and last_pos[0] == "助詞":
        if last_surface in ["が", "と", "に", "を", "は", "も"]:
            return False
            
    # Rule 3: Disallow ending with continuative form without punctuation
    has_proper_end = len(tokens[-1].part_of_speech()) > 1 and "句点" in tokens[-1].part_of_speech()[1]
    
    if not has_proper_end and len(last_pos) > 5 and "連用形" in last_pos[5]:
        return False
                
    # Rule 4: Disallow non-natural symbols in text
    full_text = "".join(t.surface() for t in tokens)
    if any(sym in full_text for sym in ["←", "→", "[[", "]]"]):
        return False
        
    # Rule 5: Disallow ending with leader or interpunct (indicates cut off or list)
    if tokens[-1].surface() in ["‥", "…", "・"]:
        return False
        
    return True

def strip_matched_brackets(text):
    pairs = [("（", "）"), ("(", ")"), ("［", "］"), ("[", "]"), ("【", "】")]
    changed = True
    while changed:
        changed = False
        for left, right in pairs:
            if text.startswith(left) and text.endswith(right):
                text = text[1:-1].strip()
                changed = True
                break
    return text

def get_japanese_density(text):
    if not text:
        return 0.0
    jp_regex = r"[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF\uFF65-\uFF9F]"
    punct_regex = r"[、。・！？!?,.]"
    
    jp_count = len(re.findall(jp_regex, text))
    punct_count = len(re.findall(punct_regex, text))
    
    return (jp_count + punct_count) / len(text)

class SentenceExtractor:
    def __init__(self):
        from sudachipy import dictionary
        from sudachipy import tokenizer
        
        self.dict = dictionary.Dictionary()
        self.tokenizer = self.dict.create()
        self.mode = tokenizer.Tokenizer.SplitMode.C

    def process_document(self, text):
        orig_count = count_kanji_kana(text)
        extracted_sentences = []
        rejected_sentences = []
        
        # Split by newline first to avoid Sudachi length limit (approx 49KB)
        for line in text.split("\n"):
            line = line.strip()
            if not line:
                continue
                
            # Check byte length for Sudachi limit (safe limit 49000 bytes)
            if len(line.encode("utf-8")) > 49000:
                # Fallback: chunk by characters (safe limit 10000 chars)
                chunks = [line[i:i+10000] for i in range(0, len(line), 10000)]
            else:
                chunks = [line]
                
            for chunk in chunks:
                if not chunk:
                    continue
                try:
                    tokens = self.tokenizer.tokenize(chunk, self.mode)
                    current_sentence_tokens = []
                    prev_was_conclusive = False
                    prev_was_predicate = False
                    has_predicate = False
                    in_quote = False
                    
                    for word in tokens:
                        surface = word.surface()
                        current_sentence_tokens.append(word)
                        pos = word.part_of_speech()
                        
                        # Quote tracking
                        if surface in ["「", "｢"]:
                            in_quote = True
                        elif surface in ["」", "｣"]:
                            in_quote = False
                        
                        # Check if conclusive form (終止形)
                        is_conclusive = len(pos) > 5 and "終止形" in pos[5]
                        
                        # Check if verb or adjective or copula (predicate)
                        is_predicate = len(pos) > 0 and (pos[0] == "動詞" or pos[0] == "形容詞" or pos[0] == "助動詞")
                        if is_predicate:
                            has_predicate = True
                        
                        # Split conditions (Only split if NOT inside quotes)
                        should_split = False
                        if len(pos) > 1 and "句点" in pos[1] and not in_quote:  # 。
                            should_split = True
                        elif len(pos) > 0 and "補助記号" in pos[0] and prev_was_conclusive and not in_quote:  # 🐎
                            should_split = True
                        elif len(pos) > 0 and "空白" in pos[0] and prev_was_conclusive and not in_quote:  # space
                            should_split = True
                        elif EMOJI_REGEX.match(surface) and prev_was_predicate and not in_quote:  # Emoji after predicate
                            should_split = True
                            
                        if should_split:
                            s = "".join(t.surface() for t in current_sentence_tokens).strip()
                            if s:
                                if has_predicate and is_valid_sentence(current_sentence_tokens):
                                    extracted_sentences.append(s)
                                else:
                                    rejected_sentences.append(s)
                            current_sentence_tokens = []
                            has_predicate = False
                            
                        prev_was_conclusive = is_conclusive
                        prev_was_predicate = is_predicate
                        
                    if current_sentence_tokens:
                        s = "".join(t.surface() for t in current_sentence_tokens).strip()
                        if s:
                            if has_predicate and is_valid_sentence(current_sentence_tokens):
                                extracted_sentences.append(s)
                            else:
                                rejected_sentences.append(s)
                            
                except Exception as e:
                    logging.error(f"Error tokenizing chunk: {e}")
                    rejected_sentences.append(chunk)

        recv_count = 0
        accepted_sentences = []
        
        # First pass: Calculate counts on ALL extracted sentences (preserving duplicates for ratio)
        for s in extracted_sentences:
            if starts_with_valid_char(s) and contains_hiragana_kana(s):
                s_clean = re.sub(r"^[＞>]+", "", s).strip()
                s_clean = strip_matched_brackets(s_clean)
                
                # Filter by Japanese character density (>= 75%)
                if get_japanese_density(s_clean) >= 0.75:
                    recv_count += count_kanji_kana(s_clean)
                    accepted_sentences.append((s_clean, s))
                else:
                    rejected_sentences.append(s)
            else:
                rejected_sentences.append(s)
                
        # Second pass: Yield UNIQUE sentences (deduplicate in document scope)
        seen_in_doc = set()
        final_accepted = []
        for s_clean, s_orig in accepted_sentences:
            if s_clean not in seen_in_doc:
                seen_in_doc.add(s_clean)
                final_accepted.append(s_clean)
            
        return final_accepted, rejected_sentences, orig_count, recv_count
