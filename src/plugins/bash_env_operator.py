from airflow.operators.bash_operator import BashOperator
from airflow.utils.decorators import apply_defaults


class BashEnvOperator(BashOperator):
    """ The regular BashOperator can have extra environment var
        using the 'env' (templated) param. However, it is not
        possible to use a callable to expand the environment vars
        with an extra dict.
    """

    @apply_defaults
    def __init__(self, *args, **kwargs):
        env_expander = kwargs.pop("env_expander")
        super().__init__(*args, **kwargs)
        # Now add our extra env by calling the env_expander
        if env_expander is not None:
            self.env = {**self.env, **env_expander()}
