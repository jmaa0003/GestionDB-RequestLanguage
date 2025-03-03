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
            raise ValueError
        else:
            with open(f"{table_name}.table", "x") as tb:
                pass #TODO écrire avec header ...
            


    def list_tables(self) -> list[str]:
        """renvoie une liste avec le nom de toutes les tables existant dans cette DB"""
        list_of_names = []
        for pseudo_table in os.listdir():
            if pseudo_table.endswith(".table"):
                list_of_names.append(pseudo_table.rsplit(".table")[0])
        return list_of_names
    
    
    def delete_table(self, table_name: str) -> None:
        """supprime la table de nom table_name."""  
        pass


    def get_table_signature(self, table_name: str) -> TableSignature:
        pass


    