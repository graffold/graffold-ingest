"""Data source connectors."""

from .api import ApiConnector
from .base import Connector, Document, ExtractionResult
from .csv import CsvConnector
from .database import DatabaseConnector
from .pdf import PdfConnector
from .sparselink import SparseLinkConnector
from .web import WebConnector

CONNECTORS: dict[str, type] = {
    "web": WebConnector,
    "pdf": PdfConnector,
    "api": ApiConnector,
    "csv": CsvConnector,
    "database": DatabaseConnector,
    "sparselink": SparseLinkConnector,
}

__all__ = [
    "ApiConnector",
    "CONNECTORS",
    "Connector",
    "CsvConnector",
    "DatabaseConnector",
    "Document",
    "ExtractionResult",
    "PdfConnector",
    "SparseLinkConnector",
    "WebConnector",
]
