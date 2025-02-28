from binary import BinaryFile
from enum import Enum

class FieldType(Enum):
    INTEGER, STRING = (1).to_bytes(), (2).to_bytes()


class Database:
    TableSignature = list[tuple[str, FieldType]]
    
    def __init__(self, name: str):
        self.name = name
        

    def create_table(self, table_name: str, *fields: TableSignature) -> None:
        """crée une nouvelle table de nom table_name et de signature fields"""


    def list_tables(self) -> list[str]:
        """ renvoie une liste avec le nom de toutes les tables existant dans cette DB"""
        offset_tb_signature, returned_name, returned_list = 9, "", []
        offset_length_then_name = self.BinaryFile.read_integer_from(2, offset_tb_signature) + 1
        for i in range(offset_length_then_name):
            returned_name += self.BinaryFile.read_string()
        
        #TODO: faire les fonctions dans l'ordre des test + boucle de renvoie noms list_tables
        
    
    def delete_table(self, table_name: str) -> None:
        """supprime la table de nom table_name. Renvoie VlaueError Si l’opération est impossible """
       

    def get_table_signature(self, table_name: str) -> TableSignature:
