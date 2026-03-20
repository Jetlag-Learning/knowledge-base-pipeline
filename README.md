# Knowledge Base Pipeline

A pipeline that syncs raw document updates to a RAG-powered chatbot's knowledge base, maintaining both document storage and vector embeddings for semantic search.

## Overview

This pipeline automates the ingestion and processing of documents into a RAG (Retrieval-Augmented Generation) knowledge base system. It handles the complete workflow from document source to vector store:

1. **Document Ingestion**: Fetches raw documents from Google Drive
2. **Document Processing**: Extracts and processes document content
3. **Chunking**: Splits documents into semantically meaningful chunks with overlap
4. **Document Store**: Persists documents and chunks in a relational database for metadata and full-text retrieval
5. **Embedding Generation**: Creates vector embeddings for each chunk using an embedding model
6. **Vector Store**: Indexes embeddings in a vector database for semantic similarity search
7. **Knowledge Base Sync**: Keeps the document store and vector store synchronized with source updates
8. **Cleanup**: Automatically clears temporary files to prevent mixing documents from different sources (prod/dev)

## Prerequisites

- Python 3.12+
- Google Cloud SQL MySQL instance
- Google Drive folder with documents
- Pinecone account and index
- OpenAI API key

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/rdndelgado/knowledge-base-pipeline.git
cd knowledge-base-pipeline
```

### 2. Create and activate virtual environment

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Linux/Mac:
source venv/bin/activate
# On Windows:
# venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Download NLTK data

```bash
python -m nltk.downloader punkt_tab
```

### 5. Configure environment variables

Create a `.env` file in the root directory with the following variables:

```env
# Google Drive
CREDENTIALS_JSON_FILE=path/to/your/service-account-credentials.json
GOOGLE_DRIVE_FOLDER_ID=your-google-drive-folder-id

# MySQL (Google Cloud SQL)
INSTANCE_CONNECTION_NAME=project:region:instance
MYSQL_DATABASE=your_database_name
MYSQL_USER=your_mysql_user
MYSQL_PASSWORD=your_mysql_password
DOCUMENT_TABLE=kb_documents
CHUNK_TABLE=kb_document_chunks

# Pinecone
PINECONE_API_KEY=your_pinecone_api_key
PINECONE_INDEX_NAME=your_index_name

# OpenAI
OPENAI_API_KEY=your_openai_api_key
OPENAI_MODEL=gpt-4o-mini
EMBEDDING_MODEL=text-embedding-3-small
```

### 6. Set up Google Drive service account

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a service account and download the JSON credentials file
3. Share your Google Drive folder with the service account email
4. Place the credentials file in your project directory
5. Update `CREDENTIALS_JSON_FILE` in your `.env` file

## Usage

### Basic Usage

1. Open `app.py` and configure your operation:

```python
def run(self):
    # Choose one operation:
    
    # Option 1: Sync documents
    self.sync(all=True)  # Sync all documents from Google Drive
    
    # Option 2: Cleanup documents
    # self.cleanup()  # Remove documents from database and Pinecone
```

2. Set which files to process (if not syncing all):

```python
self.requested_files = [
    "document-name-1",
    "document-name-2"
]
```

3. Run the pipeline:

```bash
python app.py
```

### Sync Options

**Sync all documents:**
```python
self.sync(all=True)
```

**Sync specific documents:**
```python
self.requested_files = ["document-1", "document-2"]
self.sync(all=False)
```

### Cleanup

To remove documents from the database and Pinecone:

```python
# Set which files to remove
self.requested_files = ["document-1", "document-2"]
# Or get all files from Google Drive
# self.requested_files = self.google_drive_service.fetch_files(all=True)

# Run cleanup
self.cleanup()
```

## How It Works

The pipeline follows a standard RAG knowledge base architecture:

1. **Document Ingestion**: Downloads raw documents (`.docx` files) from a Google Drive folder configured as the source
2. **Content Extraction**: Extracts text content from documents while preserving structure
3. **Text Chunking**: Splits documents into smaller, semantically coherent chunks (500-800 tokens) with 80-token overlap to maintain context across boundaries
4. **Document Store**: Persists documents and their chunks in MySQL, maintaining relationships and metadata for:
   - Full document retrieval
   - Chunk-to-document relationships
   - Metadata queries
   - Document versioning and updates
5. **Embedding Generation**: Generates dense vector embeddings for each chunk using an embedding model, converting text into numerical representations suitable for semantic search
6. **Vector Store Indexing**: Upserts embeddings into Pinecone vector database, enabling:
   - Semantic similarity search
   - Nearest neighbor retrieval
   - Context retrieval for RAG queries
7. **Synchronization**: Ensures document store and vector store remain synchronized, updating embeddings when documents change
8. **Cleanup**: Removes temporary local files after processing to prevent document mixing between different knowledge base sources (production vs development)

## Project Structure

```
knowledge-base-pipeline/
├── app.py                 # Main application entry point
├── services/
│   ├── google_drive_service.py
│   ├── document_service.py
│   ├── mysql_service.py
│   ├── embedding_service.py
│   ├── pinecone_service.py
│   └── cleanup_service.py
├── models/
│   └── database.py
├── utils/
│   ├── logger.py
│   └── tokens.py
├── documents/             # Temporary download directory (auto-cleared)
├── requirements.txt
└── .env                  # Environment variables (not in git)
```

## Knowledge Base Documents

The source documents for the knowledge base are available on Google Drive:

[KB Documents Folder](https://drive.google.com/drive/u/1/folders/16TT0FYA8T9i5Zv--0EnKXYCcHFAFB2We)

## Important Notes

- The `documents/` directory is automatically cleared after each sync/cleanup to prevent mixing documents from different Google Drive folders (prod/dev)
- Documents are matched by title (filename without extension)
- If a document doesn't exist in the database, it will be inserted. If it exists, its content will be updated
- Chunks are automatically deleted when their parent document is deleted (CASCADE)

## Troubleshooting

**NLTK data not found:**
```bash
python -m nltk.downloader punkt_tab
```

**Google Drive authentication fails:**
- Ensure the service account JSON file path is correct
- Verify the service account has access to the Google Drive folder

**MySQL connection errors:**
- Check your Cloud SQL instance connection name format: `project:region:instance`
- Verify network connectivity and credentials

**Pinecone sync issues:**
- Verify your API key and index name are correct
- Ensure the index exists in your Pinecone account

## Technologies Used

This pipeline implements a RAG knowledge base using the following technology stack:

### Document Source
- **Google Drive API**: Document source and version control

### Document Processing
- **python-docx**: Word document parsing and content extraction
- **NLTK**: Natural language processing for sentence tokenization

### Document Store (Metadata & Full-Text)
- **Google Cloud SQL (MySQL)**: Relational database for document and chunk storage
- **Cloud SQL Python Connector**: Secure connection to Cloud SQL instances
- **PyMySQL**: MySQL database driver
- **DBUtils**: Connection pooling for database operations

### Embedding Generation
- **OpenAI API**: Embedding model for vector generation
- **tiktoken**: Token counting and management

### Vector Store
- **Pinecone**: Managed vector database for semantic search

### Infrastructure & Utilities
- **Python 3.12+**: Runtime environment
- **python-dotenv**: Environment variable management
- **Logging**: Structured logging for pipeline operations
