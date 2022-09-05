from airflow.operators.bash import BashOperator


class BashEnvOperator(BashOperator):
    """The regular BashOperator can have extra environment var
    using the 'env' (templated) param. However, it is not
    possible to use a callable to expand the environment vars
    with an extra dict.
    """

    def __init__(self, *args, **kwargs):
        self.env_expander = kwargs.pop("env_expander")
        self.args = args
        self.kwargs = kwargs
        super().__init__(*self.args, **self.kwargs)

    def execute(self, context):
        # Now add our extra env by calling the env_expander
        if self.env_expander is not None:
            self.env = self.env | self.env_expander(context)
        super().execute(context)
