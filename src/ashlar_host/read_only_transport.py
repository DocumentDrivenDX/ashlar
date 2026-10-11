"""Stable exact native reader type; no journal initialization or mutations."""
import sqlite3
from urllib.parse import quote
from .delta_custody import LocalDeltaTransport, LocalDeltaError


class ReadOnlyDeltaTransport(LocalDeltaTransport):
    def _journal(self, registry, *, create):
        if create:
            raise LocalDeltaError('Read-only consumer cannot initialize')
        self.db = sqlite3.connect('file:' + quote(str(self.journal_path.resolve())) + '?mode=ro', uri=True)
        if self.db.execute('SELECT original FROM local_installation WHERE id=1').fetchall() != [(registry,)]:
            raise LocalDeltaError('Original journal registration differs')

    def mutation(self, *args, **kwargs):
        raise LocalDeltaError('Read-only publication consumer')

    def recover(self, *args, **kwargs):
        raise LocalDeltaError('Read-only publication consumer')


class ReadOnlyTransport:
    @staticmethod
    def open(spark, journal, installation, targets, policy, *, capacity=None):
        return ReadOnlyDeltaTransport(spark, journal, installation, targets, policy, capacity=capacity)
