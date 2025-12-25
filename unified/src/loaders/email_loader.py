"""
Email loader.
Loads data from email attachments via IMAP.
"""
from __future__ import annotations

import email
import imaplib
import os
import zipfile
from datetime import datetime, timedelta
from email.header import decode_header
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

import pandas as pd

from ..core.base import BaseLoader, LoaderResult

if TYPE_CHECKING:
    from ..core.schemas import ClientConfig, LoaderConfig


class EmailLoader(BaseLoader):
    """Loads data from email attachments."""

    @property
    def name(self) -> str:
        return "email"

    def load(self) -> LoaderResult:
        # Get connection parameters
        server = self.params.get("server", os.getenv("EMAIL_SERVER", ""))
        port = self.params.get("port", int(os.getenv("EMAIL_PORT", "993")))
        email_addr = self.params.get("email", os.getenv("EMAIL_USER", ""))
        password = self.params.get("password", os.getenv("EMAIL_PASSWORD", ""))
        folder = self.params.get("folder", "INBOX")

        # Search criteria
        subject_filter = self.params.get("subject_filter", "")
        sender_filter = self.params.get("sender_filter", "")
        days_back = self.params.get("days_back", 7)
        attachment_pattern = self.params.get("attachment_pattern", "*.csv")

        if not all([server, email_addr, password]):
            return LoaderResult(
                data=pd.DataFrame(),
                metadata={"error": "Email credentials not configured"},
            )

        try:
            # Connect to IMAP server
            mail = imaplib.IMAP4_SSL(server, port)
            mail.login(email_addr, password)
            mail.select(folder)

            # Build search criteria
            search_criteria = self._build_search_criteria(
                subject_filter, sender_filter, days_back
            )

            # Search for messages
            status, message_ids = mail.search(None, search_criteria)
            if status != "OK" or not message_ids[0]:
                mail.logout()
                return LoaderResult(
                    data=pd.DataFrame(),
                    metadata={"error": "No matching emails found"},
                )

            # Get the most recent message
            ids = message_ids[0].split()
            latest_id = ids[-1]

            # Fetch message
            status, msg_data = mail.fetch(latest_id, "(RFC822)")
            if status != "OK":
                mail.logout()
                return LoaderResult(
                    data=pd.DataFrame(),
                    metadata={"error": "Failed to fetch email"},
                )

            # Parse email
            msg = email.message_from_bytes(msg_data[0][1])
            mail.logout()

            # Extract attachment
            with TemporaryDirectory() as temp_dir:
                attachment_path = self._extract_attachment(
                    msg, Path(temp_dir), attachment_pattern
                )

                if not attachment_path:
                    return LoaderResult(
                        data=pd.DataFrame(),
                        metadata={"error": "No matching attachment found"},
                    )

                # Load data from attachment
                df = self._load_attachment(attachment_path)

                # Normalize column names
                df.columns = [str(c).strip().upper() for c in df.columns]

                return LoaderResult(
                    data=df,
                    metadata={
                        "rows": len(df),
                        "columns": list(df.columns),
                        "source": f"email:{self._decode_subject(msg)}",
                        "attachment": attachment_path.name,
                    },
                )

        except Exception as e:
            return LoaderResult(
                data=pd.DataFrame(),
                metadata={"error": f"Email loading failed: {e}"},
            )

    def _build_search_criteria(
        self, subject: str, sender: str, days_back: int
    ) -> str:
        """Build IMAP search criteria."""
        criteria = []

        if subject:
            criteria.append(f'SUBJECT "{subject}"')
        if sender:
            criteria.append(f'FROM "{sender}"')
        if days_back > 0:
            since_date = (datetime.now() - timedelta(days=days_back)).strftime("%d-%b-%Y")
            criteria.append(f'SINCE "{since_date}"')

        return " ".join(criteria) if criteria else "ALL"

    def _decode_subject(self, msg: email.message.Message) -> str:
        """Decode email subject."""
        subject = msg.get("Subject", "")
        decoded = decode_header(subject)
        if decoded:
            text, encoding = decoded[0]
            if isinstance(text, bytes):
                return text.decode(encoding or "utf-8", errors="replace")
            return str(text)
        return subject

    def _extract_attachment(
        self, msg: email.message.Message, temp_dir: Path, pattern: str
    ) -> Path | None:
        """Extract matching attachment from email."""
        import fnmatch

        for part in msg.walk():
            if part.get_content_maintype() == "multipart":
                continue

            filename = part.get_filename()
            if not filename:
                continue

            # Decode filename
            decoded = decode_header(filename)
            if decoded:
                text, encoding = decoded[0]
                if isinstance(text, bytes):
                    filename = text.decode(encoding or "utf-8", errors="replace")
                else:
                    filename = str(text)

            # Check if filename matches pattern
            if fnmatch.fnmatch(filename.lower(), pattern.lower()):
                filepath = temp_dir / filename
                with open(filepath, "wb") as f:
                    f.write(part.get_payload(decode=True))
                return filepath

        return None

    def _load_attachment(self, path: Path) -> pd.DataFrame:
        """Load data from attachment file."""
        suffix = path.suffix.lower()
        encoding = self.params.get("encoding", "utf-8-sig")
        separator = self.params.get("separator", ";")

        if suffix == ".zip":
            return self._load_from_zip(path, encoding, separator)
        elif suffix == ".csv":
            return pd.read_csv(
                path, sep=separator, encoding=encoding, dtype=str, low_memory=False
            )
        elif suffix in (".xlsx", ".xls"):
            return pd.read_excel(path, dtype=str)
        else:
            raise ValueError(f"Unsupported attachment type: {suffix}")

    def _load_from_zip(
        self, zip_path: Path, encoding: str, separator: str
    ) -> pd.DataFrame:
        """Load data from ZIP attachment."""
        with zipfile.ZipFile(zip_path, "r") as zf:
            for name in zf.namelist():
                lower = name.lower()
                if lower.endswith(".csv"):
                    with zf.open(name) as f:
                        return pd.read_csv(
                            f, sep=separator, encoding=encoding, dtype=str, low_memory=False
                        )
                elif lower.endswith((".xlsx", ".xls")):
                    with zf.open(name) as f:
                        return pd.read_excel(f, dtype=str)

        raise ValueError(f"No data file found in ZIP: {zip_path}")


def create_email_loader(config: LoaderConfig, client_config: ClientConfig) -> EmailLoader:
    """Factory function to create an EmailLoader."""
    return EmailLoader(config, client_config)
