from binary import BinaryFile
from enum import Enum
from math import log2, ceil
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
                    listtb_signature.append((temp_name, temp_field_type))
                return listtb_signature
            
        except FileNotFoundError:
            raise ValueError(f"{table_name}.table does not stand in this directory.")
        
    
    def add_entry(self, table_name: str, entry: Entry) -> None:
        """ajoute l’entrée entry à la table de nom table_name."""
        with open(f"{table_name}.table", "wb+") as tb:
            table_file = BinaryFile(tb)
            START_ENTRY_BUFFER = self.get_offset_entry_buffer(table_name)
            PREVIOUS_ID_ENTRY = table_file.read_integer_from(4, START_ENTRY_BUFFER)
            PREVIOUS_AMOUNT_ENTRIES = table_file.read_integer_from(4, START_ENTRY_BUFFER + 4)
            FIRST_ENTRY_POINTER = table_file.read_integer_from(4, START_ENTRY_BUFFER + 4*2)
            LAST_ENTRY_POINTER = table_file.read_integer_from(4, START_ENTRY_BUFFER + 4*3)
            CURRENT_ID_ENTRY = table_file.read_integer_from(4, START_ENTRY_BUFFER + 4*5)
            OFFSET_NEW_ENTRY = 4*(len(entry) + 8)
            
            table_file.write_integer_to(PREVIOUS_ID_ENTRY + 1, 4, START_ENTRY_BUFFER)
            table_file.write_integer_to(PREVIOUS_AMOUNT_ENTRIES + 1, 4, START_ENTRY_BUFFER + 4 )
            if not PREVIOUS_ID_ENTRY:
                FIRST_ENTRY_POINTER = LAST_ENTRY_POINTER = START_ENTRY_BUFFER + 5*4
                table_file.write_integer_to(FIRST_ENTRY_POINTER, 4, START_ENTRY_BUFFER + 4*2)
                table_file.write_integer_to(LAST_ENTRY_POINTER, 4, START_ENTRY_BUFFER + 4*3)
            else:
                table_file.write_integer_to(LAST_ENTRY_POINTER + OFFSET_NEW_ENTRY, 4, START_ENTRY_BUFFER + 4*3)
            
            table_file.write_integer_to(CURRENT_ID_ENTRY + 1, 4, START_ENTRY_BUFFER + 4*5)
            CURSOR_AS_OF_NOW = START_ENTRY_BUFFER + 4*6 
            table_file.goto(CURSOR_AS_OF_NOW)

            for entry_name in entry.keys():
                entry_value, exponent, size_of_string_buffer = entry.get(entry_name), 0, 1
                extra_bytes_str_buffer = 2**exponent - size_of_string_buffer
                if isinstance(entry_value, str):
                    size_of_string_buffer = self.available_space_string_buffer(table_name, entry_value, True)
                    if not self.available_space_string_buffer(table_name, entry_value):
                        table_file.goto(CURSOR_AS_OF_NOW + size_of_string_buffer)
                        remaining_content_of_file = tb.read()
                        table_file.goto(CURSOR_AS_OF_NOW + size_of_string_buffer)
                        exponent = ceil(log2(1 + 2 + len(entry_value) + size_of_string_buffer))
                        table_file.write_integer(0, 2**exponent - size_of_string_buffer)
                        extra_bytes_str_buffer += 2**exponent - size_of_string_buffer
                        #Au cas où TODO Attention, probleme que les autres on eu est peut etre un 0 en plus/ moins
                        tb.write(remaining_content_of_file)
                    #REMODIFICATION DES DONNEES DU HEADER + DES POINTEURS TODO
                    START_ENTRY_BUFFER = table_file.write_integer_to(START_ENTRY_BUFFER + extra_bytes_str_buffer, 4,\
                                                                      self.get_offset_entry_buffer(table_name, extra_bytes_str_buffer))
                    self.get_offset_entry_buffer(table_name)
                    table_file.write_integer_to(START_ENTRY_BUFFER)
                    
                    FIRST_ENTRY_POINTER = table_file.read_integer_from(4, START_ENTRY_BUFFER + 4*2 + 2**exponent - size_of_string_buffer)
                    offset_first_place_str_buffer = self.get_string_buffer(table_name, True) + 4
                    # TODO Si le petit index est vers l'espace devant champs
                    first_place_str_buffer = table_file.read_integer_from(4, offset_first_place_str_buffer)
                    table_file.write_string( " " + entry_value )
                    table_file.write_integer_to(first_place_str_buffer + len(entry_value) + 1, 4, first_place_str_buffer )
                else:
                    table_file.write_integer(entry.get(entry_name), 4)
            
            table_file.write_integer_to(LAST_ENTRY_POINTER, 4, -8)
            table_file.write_integer_to(-1, 4, -4) 
            
            NEW_LAST_ENTRY_POINTER = LAST_ENTRY_POINTER + 4*(3 + len(self.get_table_signature(table_name)))
            table_file.write_integer_to(NEW_LAST_ENTRY_POINTER, 4, LAST_ENTRY_POINTER) 
            # Modifier le pointeur vers l'élément suivant, du précedent élément
            
                

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
        with open(f'{table_name}.table', 'rb+') as tb:
            table_file = BinaryFile(tb)
            pass

    
    def get_offset_entry_buffer(self, table_name: str, modif: int = None) -> int:
        """Renvoie le décalage entre le début de la table table_name et l'entry_buffer. Pratique pour le localiser
           dans la table table_name. modif donne le nombre de bytes ajoutés dans le fichier, par un agrandissement du string buffer."""
        with open(f'{table_name}.table', 'rb+') as tb:
            table_file = BinaryFile(tb)
            offset = table_file.read_integer_from(4, 4*4 + self.get_number_of_bytes_table_signature(table_name))
            return offset - 20 if not modif else offset - 20 + modif
        
    
    def get_string_buffer(self, table_name: str, get_pos: bool = False) -> bytes | int:
        """Renvoie le string_buffer. Si get_pos est donné à True, renvoie l'offset du string_buffer"""
        with open(f'{table_name}.table', 'rb+') as tb:
            table_file = BinaryFile(tb)
            position_offset_str_buffer = table_file.read_integer_from(4, 8 + self.get_number_of_bytes_table_signature(table_name))
            print(self.get_offset_entry_buffer(table_name), )
            steps = self.get_offset_entry_buffer(table_name) - table_file.read_integer_from(4, position_offset_str_buffer)
            table_file.goto(position_offset_str_buffer)
            string_buffer = tb.read(steps)
            return string_buffer if not get_pos else position_offset_str_buffer 

    
    def get_number_of_bytes_table_signature(self, table_name: str) -> int:
        """Renvoie le nombre de bytes dans la signature de la table dans le header"""
        with open(f'{table_name}.table', 'rb+') as tb:
            table_file, length_table_signature = BinaryFile(tb), 0
            for field_name, field_type in self.get_table_signature(table_name):
                length_table_signature += 1 + len(field_name) + 2
            return length_table_signature


    def available_space_string_buffer(self, table_name: str, entry_value: str, give_size: bool = False) -> bool | int:
        """Renvoie True s'il y a de la place pour entry_value dans le string_buffer, False sinon. 
            Si give_size est donné à True, renvoie la taille du string_buffer."""
        available_space, i = 0, -1
        string_buffer_to_iterate = self.get_string_buffer(table_name)
        while abs(i) <= len(string_buffer_to_iterate):
            while string_buffer_to_iterate[i] == b'\x00':
                available_space += 1
                i -= 1
            else:
                available_space = 0
                i += 1
        return len(entry_value) + 2 == available_space - 1 if not give_size and isinstance(entry_value, str)\
               else len(string_buffer_to_iterate)
    