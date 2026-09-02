"""Data source connectors."""

from .agteria import AgteriaConnector
from .api import ApiConnector
from .base import Connector, Document, ExtractionResult
from .csv import CsvConnector
from .database import DatabaseConnector
from .europepmc import EuropePMCConnector
from .pdf import PdfConnector
from .pubmed import PubMedConnector
from .sparselink import SparseLinkConnector
from .web import WebConnector

CONNECTORS: dict[str, type] = {
    "agteria": AgteriaConnector,
    "web": WebConnector,
    "pdf": PdfConnector,
    "api": ApiConnector,
    "csv": CsvConnector,
    "database": DatabaseConnector,
    "sparselink": SparseLinkConnector,
    "pubmed": PubMedConnector,
    "europepmc": EuropePMCConnector,
}

__all__ = [
    "AgteriaConnector",
    "ApiConnector",
    "CONNECTORS",
    "Connector",
    "CsvConnector",
    "DatabaseConnector",
    "Document",
    "EuropePMCConnector",
    "ExtractionResult",
    "PdfConnector",
    "PubMedConnector",
    "SparseLinkConnector",
    "WebConnector",
]
