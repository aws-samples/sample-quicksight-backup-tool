"""Exceptions raised by the restore workflow."""


class RestoreError(Exception):
    """Base class for restore-specific failures."""


class RestoreConfigurationError(RestoreError):
    """Restore configuration is invalid."""


class RestoreCatalogError(RestoreError):
    """A source backup artifact could not be cataloged."""


class CatalogAccessDeniedError(RestoreCatalogError):
    """Access to a required source artifact was denied."""


class CatalogNotFoundError(RestoreCatalogError):
    """A required source artifact does not exist."""


class CatalogAmbiguityError(RestoreCatalogError):
    """Legacy artifacts cannot be associated with one run safely."""


class ArchiveValidationError(RestoreCatalogError):
    """An asset bundle is malformed, unsafe, or unsupported."""


class RestorePlanningError(RestoreError):
    """A deterministic, safe restore plan cannot be produced."""


class PlanIntegrityError(RestoreError):
    """A persisted plan does not match its digest or current configuration."""


class RestoreExecutionError(RestoreError):
    """Restore execution could not be completed."""
