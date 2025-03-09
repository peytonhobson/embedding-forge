import spacy

# LangChain imports
from langchain.schema import Document


nlp = spacy.load("en_core_web_sm")


def clean_text(text):
    """Clean and normalize text using spaCy."""
    doc = nlp(text)
    return " ".join(token.text for token in doc)


def chunk_documents(docs, chunk_size=700, overlap_ratio=0.3):
    """
    Splits each Document's page_content into fixed-size token chunks using spaCy sentence segmentation.
    Applies an overlap between consecutive chunks for context preservation.
    """
    all_chunks = []
    for doc in docs:
        cleaned_content = clean_text(doc.page_content)
        spacy_doc = nlp(cleaned_content)
        sentences = [sent.text.strip() for sent in spacy_doc.sents if sent.text.strip()]

        chunks = []
        current_chunk = []
        current_tokens = 0
        overlap_size = int(chunk_size * overlap_ratio)

        for sent in sentences:
            # Use spaCy tokenization for word count
            sent_tokens = [token.text for token in nlp(sent)]
            sent_len = len(sent_tokens)

            if current_tokens + sent_len > chunk_size and current_chunk:
                # Finish the current chunk and apply overlap
                chunks.append(" ".join(current_chunk))
                current_chunk = (
                    current_chunk[-overlap_size:]
                    if overlap_size < len(current_chunk)
                    else current_chunk
                )
                current_tokens = len(current_chunk)

            current_chunk.extend(sent_tokens)
            current_tokens += sent_len

        if current_chunk:
            chunks.append(" ".join(current_chunk))

        # Wrap each chunk into a Document (preserving metadata if available)
        for chunk in chunks:
            # Create a new document with the chunk content and COPY the original metadata
            chunk_doc = Document(
                page_content=chunk,
                metadata=(doc.metadata.copy() if hasattr(doc, "metadata") else {}),
            )
            all_chunks.append(chunk_doc)
    return all_chunks
