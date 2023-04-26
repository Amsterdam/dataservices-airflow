from typing import Final

CHANGE_DATA_TYPE: Final = """
    {% for column in params.colname %}
        ALTER TABLE {{ params.tablename }} ALTER COLUMN {{ column }} TYPE {{ params.coltype }};
    {% endfor %}
"""

ADD_LEADING_ZEROS: Final = """
    {% for column in params.colname %}
        UPDATE {{ params.tablename }}
        SET {{ column }} = LPAD({{ column }}, {{ params.num_of_zero }}, '0');
    {% endfor %}
"""
