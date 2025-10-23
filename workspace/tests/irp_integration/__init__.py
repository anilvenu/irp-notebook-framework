"""
IRP Integration test package

This package contains tests for the irp_integration module, including:
- End-to-end workflow tests against Moody's API
- Unit tests for client error handling and retry logic
- Integration tests for individual managers (EDM, Portfolio, Analysis, etc.)

These tests are separate from the main database-focused tests because they
interact with external APIs and have different requirements.
"""
