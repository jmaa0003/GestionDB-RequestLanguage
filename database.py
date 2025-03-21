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
        if not os.path.exists(self.name):
            os.mkdir(self.name)


    def create_table(self, table_name: str, *fields: TableSignature) -> None:
        """Crée une nouvelle table de nom table_name et de signature fields."""
        if os.path.exists(f"{table_name}.table"):
            raise ValueError(f'{table_name}.table already stands in this directory')
        with open(f"{self.name}/{table_name}.table", "wb+") as tb:
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
            for i in range(len(fields)):
                table_file.write_integer(0, 4)
            for i in range(2):   
                table_file.write_integer(-1, 4) #deux derniers pointeurs d'élément de la liste chaînée


    def list_tables(self) -> list[str]:
        """Renvoie une liste avec le nom de toutes les tables existant dans cette DB"""
        list_of_names = []
        for pseudo_table in os.listdir(self.name):
            if pseudo_table.endswith(".table"):
                list_of_names.append(pseudo_table.rsplit(".table")[0])
        return list_of_names
    
     
    def delete_table(self, table_name: str) -> None:
        """Supprime la table de nom table_name.""" 
        try:
            os.remove(f"{self.name}/{table_name}.table")
        except FileNotFoundError:
            raise ValueError(f"{table_name}.table does not stand in this path.")
        

    def get_table_signature(self, table_name: str) -> TableSignature:
        """Renvoie la signature de la table de type TableSignature"""
        try:
            with open(f"{table_name}.table", "rb+") as tb:
                table_file, listtb_signature = BinaryFile(tb), []
                table_file.goto(4)
                NUMBER_OF_FIELDS = table_file.read_integer(4)
                for i in range(NUMBER_OF_FIELDS):
                    index_field_type = table_file.read_integer(1)
                    temp_field_type, temp_name = list(FieldType)[index_field_type - 1], table_file.read_string()
                    print(temp_field_type)
                    listtb_signature.append((temp_name, temp_field_type))
                return listtb_signature
            
        except FileNotFoundError:
            raise ValueError(f"{table_name}.table does not stand in this directory.")
        
    
    def add_entry(self, table_name: str, entry: Entry) -> None:
        """ajoute l’entrée entry à la table de nom table_name."""
        with open(f"{table_name}.table", "wb") as tb:
            table_file = BinaryFile(tb)
            START_ENTRY_BUFFER = self.get_offset_entry_buffer(table_name)
            PREVIOUS_ID_ENTRY = table_file.read_integer_from(4, START_ENTRY_BUFFER)
            PREVIOUS_AMOUNT_ENTRIES = table_file.read_integer_from(4, START_ENTRY_BUFFER + 4)
            FIRST_ENTRY_POINTER = table_file.read_integer_from(4, START_ENTRY_BUFFER + 4*2)
            LAST_ENTRY_POINTER = table_file.read_integer_from(4, START_ENTRY_BUFFER + 4*3)
            CURRENT_ID_ENTRY = table_file.read_integer_from(4, START_ENTRY_BUFFER + 4*5)
            OFFSET_ELEMENT_POINTERS, OFFSET_NEW_ENTRY = START_ENTRY_BUFFER + 4 * len(entry), 4*(len(entry) + 8)
            PREVIOUS_ELEMENT_POINTER = table_file.read_integer_from(4, OFFSET_ELEMENT_POINTERS)
            NEXT_ELEMENT_POINTER = table_file.read_integer_from(4, OFFSET_ELEMENT_POINTERS + 4)
            
            table_file.write_integer_to(PREVIOUS_ID_ENTRY + 1, 4, START_ENTRY_BUFFER)
            table_file.write_integer_to(PREVIOUS_AMOUNT_ENTRIES + 1, 4, START_ENTRY_BUFFER + 4 )
            if not PREVIOUS_ID_ENTRY:
                FIRST_ENTRY_POINTER = LAST_ENTRY_POINTER = START_ENTRY_BUFFER + 5*4
                table_file.write_integer_to(FIRST_ENTRY_POINTER, 4, START_ENTRY_BUFFER + 4*2)
                table_file.write_integer_to(LAST_ENTRY_POINTER, 4, START_ENTRY_BUFFER + 4*3)
            else:
                table_file.write_integer_to(LAST_ENTRY_POINTER + OFFSET_NEW_ENTRY, 4, START_ENTRY_BUFFER + 4*3)
            
            table_file.write_integer_to(CURRENT_ID_ENTRY + 1, 4, START_ENTRY_BUFFER + 4*5)
            cursor_as_of_now = START_ENTRY_BUFFER + 4*6
            for entry_name in entry.keys():
                entry_value = entry.get(entry_name)
                if isinstance(entry_value, str):
                    #si pas de place ajouter le double de 0 (compteur à 16+ tout le fichier)
                    #écrire dans le string buffer et decaler la première place dispo et l'entry buffer (peut être aussi les pointeurs ?)
                    if not self.available_space_string_buffer(table_name, entry_value):
                        #TODO décalage dans le file

                        
                else:
                    table_file.write_integer(entry.get(entry_name), 4)
            table_file.write_integer_to(LAST_ENTRY_POINTER, 4, -8) #le pointer
            
                

    def get_complete_table(self, table_name: str) -> list[Entry]:
        """Renvoie toutes les entrées de la table de nom table_name dans une liste."""
        pass


    def get_entry(self, table_name: str, field_name: str, field_value: Field) -> Entry | None :
        """Renvoie une entrée (quelconque) de la table de nom table_name dont le champ field_name contient\
        la valeur field_value si une telle entrée existe, et qui renvoie None sinon."""
        pass


    def get_entries(self, table_name: str, field_name: str, field_value: Field) -> list[Entry]:
        """Renvoie toutes les entrées de la table de nom table_name dont le champ field_name contient la valeur field_name."""
        pass


    def select_entry(self, table_name: str, fields: tuple[str], field_name: str, field_value: Field) -> Field | tuple[Field]:
        """Effectue une sélection des champs demandés sur une entrée de la table de nom table_name dont le champ field_name contient\
        la valeur field_value et renvoie ces champs uniquement. Si un unique champ est demandé, la fonction ne doit pas
        renvoyer un tuple de taille 1, mais bien uniquement la valeur du champ demandé.
        Sinon, le tuple renvoyé doit contenir les valeurs des champs dans le même ordre que celui demandé par le paramètre fields."""
        pass


    def select_entries(self, table: str, fields: tuple[str], field_name: str, field_value: Field) -> list[Field | tuple[Field]]:
        """Similaire à select_entry cependant renvoie les champs demandés de
           toutes les entrées de la table de nom table_name satisfaisant la condition."""
        pass


    def get_table_size(self, table_name: str) -> int:
        """Renvoie le nombre d’entrées dans la table de nom table_name"""
        """with open(f'{table_name}.table', 'rb+') as tb:
            table_file = BinaryFile(tb)"""

    
    def get_offset_entry_buffer(self, table_name: str) -> int:
        """Renvoie le décalage entre le début de la table table_name et l'entry_buffer. Pratique pour le localiser
           dans la table table_name"""
        with open(f'{table_name}.table', 'rb+') as tb:
            table_file = BinaryFile(tb)
            table_file.goto(8)
            length_table_signature = 0
            for field_name, field_type in self.get_table_signature(table_name):
                length_table_signature += 1 + len(field_name) + 2
            return length_table_signature - 4
        
    
    def get_string_buffer(self, table_name: str) -> bytes:
        """Renvoie le string buffer"""
        with open(f'{table_name}.table', 'rb+') as tb:
            table_file = BinaryFile(tb)
            offset = 4*5 + self.get_number_of_bytes_table_signature(table_name)
            table_file.goto(offset)
            offset_before_end_table = ( len(self.get_table_signature(table_name)) + 8 ) * 4 
            steps = table_file.get_size() - offset_before_end_table
            string_buffer = tb.read(steps)
            return string_buffer

    
    def get_number_of_bytes_table_signature(self, table_name: str) -> int:
        """Renvoie le nombre de bytes dans la signature de la table dans le header"""
        with open(f'{table_name}.table', 'rb+') as tb:
            table_file, length_table_signature = BinaryFile(tb), 0
            for field_name, field_type in self.get_table_signature(table_name):
                length_table_signature += 1 + len(field_name) + 2
            return length_table_signature

    def available_space_string_buffer(self, table_name: str, entry_value: str) -> bool:
        """Renvoie True s'il y a de la place pour entry_value dans le string_buffer, False sinon"""
        available_space, i = 0, -1
        string_buffer_to_iterate = self.get_string_buffer(table_name)
        while abs(i) <= len(string_buffer_to_iterate):
            while string_buffer_to_iterate[i] == b'\x00':
                available_space += 1
                i -= 1
            else:
                available_space = 0
                i += 1
        return len(entry_value) + 2 == available_space - 1
    