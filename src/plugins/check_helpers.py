import re
from attrdict import AttrDict

RE_SAFE_IDENTIFIER = re.compile(r"\A[a-z][a-z0-9_\-]+\Z", re.I)


def check_safe_name(sql_identifier):
    """Check whether an identifier can safely be used inside an SQL statement.
    This avoids causing SQL injections when the identifier ever becomes user-input.
    """
    if not RE_SAFE_IDENTIFIER.match(sql_identifier):
        raise RuntimeError(f"Unsafe input used as table/field-name: {sql_identifier}")


def make_params(checks):
    return AttrDict({check.check_id: check.params for check in checks})
