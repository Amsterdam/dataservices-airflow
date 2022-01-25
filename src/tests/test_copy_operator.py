from contextlib import nullcontext
from pathlib import Path

import postgres_table_copy_operator
import pytest
from airflow.exceptions import AirflowFailException
from postgres_table_copy_operator import PostgresTableCopyOperator
from schematools.utils import dataset_schema_from_path

FILES_PATH = Path(__file__).parent / "files"
should_work = nullcontext()


def _patched_schema_getter(url, name):
    return dataset_schema_from_path(FILES_PATH / "gebieden.json")


@pytest.mark.parametrize(
    "dataset_name, sql_init_file, drop_target_if_unequal, contextmgr, "
    "target_row_count, related_row_counts",
    [
        (
            None,
            "only-new",
            True,
            should_work,
            1,
            {},
        ),
        (
            "gebieden",
            "only-new",
            False,
            should_work,
            1,
            {},
        ),
        (
            None,
            "new-and-existing",
            True,
            should_work,
            2,
            {},
        ),
        (
            "gebieden",
            "new-and-existing",
            False,
            should_work,
            2,
            {},
        ),
        (
            "gebieden",
            "new-and-existing-mixed-order",
            False,
            should_work,
            1,
            {},
        ),
        (  # Combination of no dataset (not knowing columns)
            # and not drop_target_if_unequal can lead to an exception.
            None,
            "new-and-existing-mixed-order",
            False,
            pytest.raises(AirflowFailException),
            0,
            {},
        ),
        (
            None,
            "new-and-existing-unequal",
            True,
            should_work,
            2,
            {},
        ),
        (
            "gebieden",
            "new-and-existing-unequal",
            False,
            should_work,
            2,
            {},
        ),
        (
            None,
            "with-related",
            True,
            should_work,
            1,
            {"gebieden_ggwgebieden_bestaat_uit_buurten": 1},
        ),
        (
            "gebieden",
            "with-related",
            True,
            should_work,
            1,
            {"gebieden_ggwgebieden_bestaat_uit_buurten": 1},
        ),
        (
            "gebieden",
            "with-related",
            False,
            should_work,
            1,
            {"gebieden_ggwgebieden_bestaat_uit_buurten": 1},
        ),
        (
            "gebieden",
            "with-related-unequal",
            False,
            should_work,
            1,
            {"gebieden_ggwgebieden_bestaat_uit_buurten": 1},
        ),
        (
            "gebieden",
            "new-and-existing-with-view",
            False,
            should_work,
            2,
            {},
        ),
        (
            "gebieden",
            "new-and-existing-with-view",
            True,  # drop_target_if_unequal not allowed for views
            pytest.raises(AirflowFailException),
            1,  # Only the original record
            {},
        ),
        (
            "gebieden",
            "new-and-existing-unequal-with-view",
            False,
            pytest.raises(AirflowFailException),
            1,
            {},
        ),
        (
            "gebieden",
            "with-related-unequal-with-view",
            False,
            pytest.raises(AirflowFailException),
            0,
            {},
        ),
    ],
)
def test_table_copy_operator(
    postgresql,
    mock_pghook,
    test_dag,
    monkeypatch,
    dataset_name,  # Start of parametrized arguments
    sql_init_file,
    drop_target_if_unequal,
    contextmgr,
    target_row_count,
    related_row_counts,
):
    """Prove that the `PostgresTableCopyOperator` is working correctly.

    This parametrized test first creates tables using the `sql_init_file` parameter.
    Then the operator is used with different input parameters
    (`contextmgr` and `drop_target_if_unequal).
    Finally, rowcount checks are done on the final results in the database using
    `target_row_count` and `related_row_counts`.

    The initial table setup files (`sql_init_file`), containing sql statements
    have some additional comments explaining the sql structure that is being set up.
    """
    monkeypatch.setattr(
        postgres_table_copy_operator, "dataset_schema_from_url", _patched_schema_getter
    )
    pg_cursor = postgresql.cursor()

    with postgresql.cursor() as pg_cursor:

        # Set initial table + table content
        with open(FILES_PATH / f"{sql_init_file}.sql") as sql_file:
            pg_cursor.execute(sql_file.read())
            # Need a commit here, because operator uses its own pg connection
            # that runs in a different transaction
            pg_cursor.connection.commit()

        with contextmgr:
            task = PostgresTableCopyOperator(
                dataset_name=dataset_name,
                source_table_name="gebieden_ggwgebieden_new",
                target_table_name="gebieden_ggwgebieden",
                drop_target_if_unequal=drop_target_if_unequal,
                dag=test_dag,
            )

            pytest.helpers.run_task(task=task, dag=test_dag)  # type: ignore

        pg_cursor.execute("SELECT * from gebieden_ggwgebieden")
        rows = pg_cursor.fetchall()
        assert len(rows) == target_row_count

        for table_name, row_count in related_row_counts.items():
            pg_cursor.execute(f"SELECT * from {table_name}")  # noqa: S608
            rows = pg_cursor.fetchall()
            assert len(rows) == row_count
