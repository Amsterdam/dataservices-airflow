from dataclasses import dataclass
from typing import Optional


@dataclass
class ContactPoint:
    """Represent an Amsterdam Schema ``contactPoint``.

    The Amsterdam Schema defines the ``contactPoint``, but also its attributes, to be entirely
    optional.
    """

    name: Optional[str] = None
    email: Optional[str] = None

    def __str__(self) -> str:
        """Return human readable string representation.

        We strive for ``"name <email>"``, but it can even be just
         - an empty string,
         - only the name
         - only the email address.

        Returns:
            Human readable string representation.
        """
        name = self.name if self.name else ""
        email = f"<{self.email}>" if self.email else ""
        return f"{name} {email}".strip()
