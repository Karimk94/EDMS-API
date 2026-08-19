import threading
import logging


class UploadCancelRegistry:
    """
    In-memory registry of cancellation flags for in-progress uploads,
    keyed by idempotency key.

    A flag is registered when an upload starts and checked between chunks
    while the file is being streamed to the DMS. Setting the flag (via the
    cancel endpoint or a client disconnect) causes the upload loop to abort
    and clean up the partially-created document.
    """

    def __init__(self):
        self._events = {}
        self._lock = threading.Lock()

    def register(self, key: str) -> threading.Event:
        """Register a new cancellation flag for an upload. Returns the event."""
        event = threading.Event()
        with self._lock:
            self._events[key] = event
        return event

    def cancel(self, key: str) -> bool:
        """
        Signal cancellation for the given upload key.
        Returns True if the key was found and flagged, False otherwise.
        """
        with self._lock:
            event = self._events.get(key)
        if event is not None:
            event.set()
            return True
        return False

    def is_cancelled(self, key: str) -> bool:
        """Return whether the given upload has been cancelled."""
        with self._lock:
            event = self._events.get(key)
        return event.is_set() if event is not None else False

    def unregister(self, key: str) -> None:
        """Remove the cancellation flag for an upload that has finished."""
        with self._lock:
            self._events.pop(key, None)


# Global singleton instance
upload_cancel_registry = UploadCancelRegistry()