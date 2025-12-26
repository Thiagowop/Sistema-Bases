"""
Test VIC Email Extraction
"""
import os
import sys
from pathlib import Path

# Setup path
sys.path.insert(0, str(Path(__file__).parent))

# Load .env
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")

print("=== VIC Email Extraction Test ===\n")

# Print env values (masked)
print("Environment Variables:")
print(f"  EMAIL_USER: {os.getenv('EMAIL_USER', 'NOT SET')}")
print(f"  EMAIL_APP_PASSWORD: {'*' * 8 if os.getenv('EMAIL_APP_PASSWORD') else 'NOT SET'}")
print(f"  IMAP_SERVER: {os.getenv('IMAP_SERVER', 'NOT SET')}")
print(f"  VIC_EMAIL_SENDER: {os.getenv('VIC_EMAIL_SENDER', 'NOT SET')}")
print(f"  VIC_EMAIL_SUBJECT: {os.getenv('VIC_EMAIL_SUBJECT', 'NOT SET')}")
print(f"  VIC_ATTACHMENT_FILENAME: {os.getenv('VIC_ATTACHMENT_FILENAME', 'NOT SET')}")
print()

# Test email loader
from src.loaders.email_loader import EmailLoader
from src.core.schemas import LoaderConfig, LoaderType

config = LoaderConfig(
    type=LoaderType.EMAIL,
    params={
        "server": os.getenv("IMAP_SERVER"),
        "email": os.getenv("EMAIL_USER"),
        "password": os.getenv("EMAIL_APP_PASSWORD"),
        "sender_filter": os.getenv("VIC_EMAIL_SENDER"),
        "subject_filter": os.getenv("VIC_EMAIL_SUBJECT"),
        "attachment_pattern": os.getenv("VIC_ATTACHMENT_FILENAME"),
        "days_back": 30,  # Look back 30 days
        "encoding": "utf-8-sig",
        "separator": ";",
    }
)

print("Testing Email Loader...")
loader = EmailLoader(config, None)
result = loader.load()

if "error" in result.metadata:
    print(f"\n[ERROR] {result.metadata['error']}")
else:
    print(f"\n[SUCCESS] Email extraction completed!")
    print(f"  Rows: {result.metadata.get('rows', 0)}")
    print(f"  Columns: {len(result.metadata.get('columns', []))}")
    print(f"  Source: {result.metadata.get('source', 'N/A')}")
    print(f"  Attachment: {result.metadata.get('attachment', 'N/A')}")
    
    if not result.data.empty:
        print(f"\nFirst columns: {list(result.data.columns[:10])}")
        print(f"\nSample data (first 3 rows):")
        print(result.data.head(3).to_string())
