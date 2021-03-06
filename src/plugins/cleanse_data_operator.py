import re
from airflow.models.baseoperator import BaseOperator


class CleanseDataOperator(BaseOperator):
    """ " Translates (unwanted) character(s) to a defined one.
    The parameter character_translation is dictionary containing the character to target and
    as value the translation to make i.e.
    {'[\\n\\r]', ''} (removes carriage or new lines) or
    {'[ ]{2,}', ' '} (translates more then 2 white spaces to a single white space)"""

    def __init__(
        self,
        *,
        input_file: str,
        output_file: str = None,
        character_translation: dict,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.output_file = output_file
        self.character_translation = character_translation

    def execute(self, context=None):

        # Prepare data: if outputfile is specified
        # then do translation only in output file
        if self.output_file:
            data = open(self.input_file, "r").read()
            with open(self.output_file, "w") as output:
                output.write(data)
            data = open(self.output_file, "r").read()
        else:
            data = open(self.input_file, "r").read()

        # Translate data
        for character, translation in self.character_translation.items():
            result = re.sub(r"{0}".format(character), f"{translation}", data)
            with open(self.output_file or self.input_file, "w") as output:
                output.write(result)