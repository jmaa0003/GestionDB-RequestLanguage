from binary import BinaryFile
from enum import Enum
import os

class FieldType(Enum):
    INTEGER, STRING = 1, 2


class Database:
    """Dans cette classe, toutes les méthodes renvoient ValueError si l'opération liée est impossible"""
    TableSignature = list[tuple[str, FieldType]]
    
    def __init__(self, name: str):
        self.name = name


    def create_table(self, table_name: str, *fields: TableSignature) -> None:
        """crée une nouvelle table de nom table_name et de signature fields."""
        if os.path.exists(f"{table_name}.table"):
            raise ValueError(f'{table_name}.table already stands in this path')
        else:
            with open(f"{table_name}.table", "wb+") as tb:
                table_file, initial_buffer_size = BinaryFile(tb), 16
                # SIGNATURE
                table_file.write_string('ULDB')
                table_file.write_integer(len(fields), len(fields))
                for one_field, a_field_type in fields:
                    on_x_bytes = len(one_field)
                    table_file.write_integer(a_field_type.value, FieldType.INTEGER.value)
                    table_file.write_integer(on_x_bytes, 2)
                    table_file.write_string(one_field)

                offset_string_buffer = tb.tell() + 12
                for i in range(2):
                    table_file.write_integer(offset_string_buffer, 4) #première place dans le string buffer écrite aussi
                for i in range(2):
                    table_file.write_integer(0, 4)
                for i in range(2):
                    table_file.write_integer(-1, 4)
                # STRING BUFFER
                table_file.write_integer(0, initial_buffer_size)


    def list_tables(self) -> list[str]:
        """renvoie une liste avec le nom de toutes les tables existant dans cette DB"""
        list_of_names = []
        for pseudo_table in os.listdir():
            if pseudo_table.endswith(".table"):
                list_of_names.append(pseudo_table.rsplit(".table")[0])
        return list_of_names
    
    
    def delete_table(self, table_name: str) -> None:
        """supprime la table de nom table_name.""" 
        try:
            os.remove(f"{table_name}.table")
        except FileNotFoundError:
            raise ValueError(f"{table_name}.table does not stand in this path.")
        

    def get_table_signature(self, table_name: str) -> TableSignature:
        try:
            with open(f"{table_name}.table", "rb+") as tb:
                pass #TODO
        except FileNotFoundError:
            raise ValueError(f"{table_name}.table does not stand in this path.")
