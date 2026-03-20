from services.google_drive_service import GoogleDriveService
from services.document_service import DocumentService
from services.mysql_service import MySQLService
from services.embedding_service import EmbeddingService
from services.pinecone_service import PineconeService
from services.cleanup_service import CleanupService
from utils.logger import logger
from dotenv import load_dotenv

class App:
    def __init__(self):
        self.google_drive_service = GoogleDriveService(
            download_dir="documents"
        )
        self.mysql_service = MySQLService()
        self.document_service = DocumentService(mysql_service=self.mysql_service)
        self.embedding_service = EmbeddingService()
        self.pinecone_service = PineconeService()

        self.requested_files = [
            "dev-introduction"
        ]

    def run(self):
        try:
            # ----------------ONLY CHOOSE ONE OPERATION----------------

            # --------------------------------CLEANUP----------------------------------------
            # self.requested_files = self.google_drive_service.fetch_files(all=True) # all documents
            # self.cleanup() # will remove records in SQL DB and Pinecone specified in the self.requested_files

            # --------------------------------SYNC----------------------------------------
            self.sync(all=True)  # Update documents in SQL database and Pinecone from Google Drive Folder -> change to False to sync only specific files you set in the self.requested_files list
        except Exception as e:
            logger.exception(f"App run failed: {e}")

    def sync(self, all=False):
        """Sync documents from Google Drive to MySQL and Pinecone, then clear local files."""
        try:
            # Pull documents, mapped their IDs in the DB, update and chunk them
            if not all:
                filenames = self.google_drive_service.fetch_files(titles=self.requested_files)
            else:
                filenames = self.google_drive_service.fetch_files(all=all)

            if not filenames:
                logger.warning('No files downloaded. Skipping process.')
                return
            
            mapped_docs, chunked_docs = self.document_service.process(documents=filenames)
            inserted_chunks = self.mysql_service.bulk_insert_chunks(chunked_docs)

            # Generate embeddings
            embedded_chunks = self.embedding_service.generate_embeddings(inserted_chunks)
            total_upserted = self.pinecone_service.sync(embedded_chunks)
            logger.info(f"Sync Complete.")
        finally:
            # Always clear the documents directory at the end to prevent mixing prod/dev documents
            try:
                self.google_drive_service.clear_download_dir()
            except Exception as e:
                logger.error(f"Failed to clear download directory: {e}")
                # Don't raise - we want sync to complete even if cleanup fails

    def cleanup(self):
        """Clean up documents from MySQL and Pinecone, then clear local files."""
        try:
            cleanup = CleanupService(
                mysql_service=self.mysql_service,
                pinecone_index=self.pinecone_service.index,
                requested_files=self.requested_files
            )
            cleanup.run()
        finally:
            # Always clear the documents directory at the end to prevent mixing prod/dev documents
            try:
                self.google_drive_service.clear_download_dir()
            except Exception as e:
                logger.error(f"Failed to clear download directory: {e}")
                # Don't raise - we want cleanup to complete even if file cleanup fails

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Knowledge Base Pipeline")
    parser.add_argument(
        "--operation",
        choices=["sync", "cleanup"],
        default="sync",
        help="Operation to perform (default: sync)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        default=False,
        help="Sync/cleanup all documents from Google Drive",
    )
    parser.add_argument(
        "--files",
        type=str,
        default="",
        help=(
            "Comma-separated list of Google Drive document names to process. "
            "Names must match the Google Drive file names exactly (including "
            "extensions such as .docx for uploaded Word documents, when applicable)."
        ),
    )
    args = parser.parse_args()

    load_dotenv()
    app = App()

    if args.files:
        app.requested_files = [f.strip() for f in args.files.split(",") if f.strip()]

    if args.operation == "sync":
        app.sync(all=args.all)
    elif args.operation == "cleanup":
        if args.all:
            app.requested_files = app.google_drive_service.fetch_files(all=True)
        app.cleanup()