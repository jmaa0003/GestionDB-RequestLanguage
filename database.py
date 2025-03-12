from binary import BinaryFile
from enum import Enum
import os

class FieldType(Enum):
    INTEGER, STRING = 1, 2


class Database:
    """Dans cette classe, toutes les méthodes renvoient ValueError si l'opération liée est impossible"""
    TableSignature = list[tuple[str, FieldType]]
    Field = str | int
    Entry = dict[str, Field]
    
    def __init__(self, name: str):
        self.name = name


    def create_table(self, table_name: str, *fields: TableSignature) -> None:
        """Crée une nouvelle table de nom table_name et de signature fields."""
        if os.path.exists(f"{table_name}.table"):
            raise ValueError(f'{table_name}.table already stands in this path')
        else:
            with open(f"{table_name}.table", "wb+") as tb:
                table_file = BinaryFile(tb)
                INITIAL_STR_BUFFER_SIZE = 16
                # SIGNATURE
                tb.write('ULDB'.encode())
                table_file.write_integer(len(fields), 4)
                for one_field, a_field_type in fields:
                    table_file.write_integer(a_field_type.value, FieldType.INTEGER.value)
                    table_file.write_string(one_field)
                OFFSET_STRING_BUFFER = tb.tell() + 12
                OFFSET_ENTRY_BUFFER = OFFSET_STRING_BUFFER + 16
                for i in range(2):
                    table_file.write_integer(OFFSET_STRING_BUFFER, 4) # Offset + Première place dans le string buffer
                table_file.write_integer(OFFSET_ENTRY_BUFFER, 4)
                # STRING BUFFER INITIALISED
                table_file.write_integer(0, INITIAL_STR_BUFFER_SIZE)
                # ENTRY BUFFER 
                table_file.write_integer(0, 8) # Le dernier ID utilisé + nombre total d'éntrées dans la table
                for i in range(3):
                    table_file.write_integer(-1, 4) # 3 pointeurs de l'entry buffer
                table_file.write_integer(0, 4) #début liste chaînée
                for i in range(2):   
                    table_file.write_integer(-1, 4) #deux derniers pointeurs d'élément de la liste chaînée


    def list_tables(self) -> list[str]:
        """Renvoie une liste avec le nom de toutes les tables existant dans cette DB"""
        list_of_names = []
        for pseudo_table in os.listdir():
            if pseudo_table.endswith(".table"):
                list_of_names.append(pseudo_table.rsplit(".table")[0])
        return list_of_names
    
    
    def delete_table(self, table_name: str) -> None:
        """Supprime la table de nom table_name.""" 
        try:
            os.remove(f"{table_name}.table")
        except FileNotFoundError:
            raise ValueError(f"{table_name}.table does not stand in this path.")
        

    def get_table_signature(self, table_name: str) -> TableSignature:
        try:
            with open(f"{table_name}.table", "rb") as tb:
                table_file, listtb_signature = BinaryFile(tb), []
                table_file.goto(4)
                NUMBER_OF_FIELDS = table_file.read_integer(4)
                for i in range(NUMBER_OF_FIELDS):
                    index_field_type = table_file.read_integer(1)
                    temp_field_type, temp_name = list(FieldType)[index_field_type], table_file.read_string()
                    listtb_signature.append((temp_name, temp_field_type))
                return listtb_signature
            
        except FileNotFoundError:
            raise ValueError(f"{table_name}.table does not stand in this path.")
        
    
    def add_entry(self, table_name: str, entry: Entry) -> None:
        """ajoute l’entrée entry à la table de nom table_name."""
        with open(f"{table_name}.table", "wb+") as tb:
            if isinstance(entry[1], str):
                pass
            else:
                pass


    def get_complete_table(self, table_name: str) -> list[Entry]:
        """Renvoie toutes les entrées de la table de nom table_name dans une liste."""


    def get_entry(self, table_name: str, field_name: str, field_value: Field) -> Entry | None:
        """Renvoie une entrée (quelconque) de la table de nom table_name dont le champ field_name contient\
        la valeur field_value si une telle entrée existe, et qui renvoie None sinon."""


    def get_entries(self, table_name: str, field_name: str, field_value: Field) -> list[Entry]:
        """Renvoie toutes les entrées de la table de nom table_name dont le champ field_name contient la valeur field_name."""


    def select_entry(self, table_name: str, fields: tuple[str], field_name: str, field_value: Field) -> Field | tuple[Field]:
        """Effectue une sélection des champs demandés sur une entrée de la table de nom table_name dont le champ field_name contient\
        la valeur field_value et renvoie ces champs uniquement. Si un unique champ est demandé, la fonction ne doit pas
        renvoyer un tuple de taille 1, mais bien uniquement la valeur du champ demandé.
        Sinon, le tuple renvoyé doit contenir les valeurs des champs dans le même ordre que celui demandé par le paramètre fields."""


    def select_entries(self, table: str, fields: tuple[str], field_name: str, field_value: Field) -> list[Field | tuple[Field]]:
        """Similaire à select_entry cependant renvoie les champs demandés de
           toutes les entrées de la table de nom table_name satisfaisant la condition."""
        

