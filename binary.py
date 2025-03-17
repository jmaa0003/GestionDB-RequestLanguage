from typing import BinaryIO

#TODO: J'aurai besoin de ça -> bytes.expandtabs(tabsize)
class BinaryFile:
    def __init__(self, file: BinaryIO):
        """Constructeur de BinaryFile"""
        self.file = file


    def goto(self, pos: int) -> None:
        """Déplace l’endroit pointé dans le fichier à pos bytes après le début du fichier\
            si pos est positif et à pos bytes avant la fin du fichier si est négatif"""
        if pos >= 0:
            self.file.seek(pos)
        else:
            self.file.seek(-pos, whence = 2)
        

    def get_size(self) -> int:
        """Renvoie la taille (nombre de bytes) du fichier, sans déplacer le curseur"""
        INITIAL_CURSOR = self.file.tell()
        self.goto(0)
        size_of_file = len(self.file.read())
        self.goto(INITIAL_CURSOR)
        return size_of_file
        
    

    def write_integer(self, n: int, size: int)-> int:
        """Écrit l’entier n sur size bytes à l’endroit pointé actuellement par le fichier.
            Renvoie le nombre de bytes écrits dans le fichier.
            Change l’endroit pointé par le fichier après exécution."""
        self.file.write(n.to_bytes(length = size, byteorder = 'little', signed = True ))
        return size
    

    def write_integer_to(self, n: int, size: int, pos: int)-> int:
        """Écrit l’entier n sur size bytes à la pos-ième position dans le fichier\
            ( *voir def goto() ). Renvoie le nombre de bytes écrits dans le fichier.
            Ne change pas l’endroit pointé par le fichier après exécution"""
        INITIAL_CURSOR = self.file.tell()
        self.goto(pos)
        self.file.write(n.to_bytes(length = size, byteorder = 'little', signed = True ))
        self.goto(INITIAL_CURSOR)
        return size
    

    def write_string(self, s: str)-> int:
        """Écrit la chaîne de caractère s à l’endroit pointé actuellement par le fichier.
            Renvoie le nombre de bytes écrits dans le fichier.
            Change l’endroit pointé par le fichier après exécution."""
        PREVIOUS_FILE_SIZE = self.get_size()
        number_of_bytes_needed = 0
        for character in s:
            if character.isascii():
                number_of_bytes_needed += 1
            else:
                number_of_bytes_needed += 2
        self.write_integer(number_of_bytes_needed, 2)     
        self.file.write(s.encode())
        return self.get_size() - PREVIOUS_FILE_SIZE
        

    def write_string_to(self, s: str, pos: int) -> int:
        """Écrit la chaîne de caractère s à la pos-ième* position dans le fichier\
            ( *voir def goto() ). Renvoie le nombre de bytes écrits dans le fichier.
            Ne change pas l’endroit pointé par le fichier après exécution."""
        PREVIOUS_FILE_SIZE, INITIAL_CURSOR = self.get_size(), self.file.tell()
        number_of_bytes_needed = 0
        self.goto(pos) # Le self.get_size() me permet de vérifier si seekable
        for character in s:
            if character.isascii():
                number_of_bytes_needed += 1
            else:
                number_of_bytes_needed += 2
        self.write_integer(number_of_bytes_needed, 2)
        self.file.write(s.encode())
        self.goto(INITIAL_CURSOR)
        return self.get_size() - PREVIOUS_FILE_SIZE
    

    def read_integer(self, size: int)-> int:
        """Renvoie l’entier encodé sur size bytes à partir de l’endroit pointé\
            actuellement par le fichier. Change l’endroit pointé par le fichier après exécution."""
        encoded_integer = self.file.read(size)
        return int.from_bytes(encoded_integer, byteorder = 'little', signed = True )


    def read_integer_from(self, size: int, pos: int)-> int:
        """Renvoie l’entier encodé sur size bytes à partir de la pos-ième*\
            position dans le fichier( *voir def goto() ).
        Ne change pas l’endroit pointé par le fichier après exécution."""
        INITIAL_CURSOR = self.file.tell()
        self.goto(pos)
        encoded_integer = self.file.read(size)
        self.goto(INITIAL_CURSOR)
        return int.from_bytes(encoded_integer, byteorder = 'little', signed = True )
        

    def read_string(self)-> str:
        """Renvoie la chaîne de caractères encodée à\
            l’endroit pointé actuellement par le fichier.
            Change l’endroit pointé par le fichier après exécution."""
        encoded_string = bytearray()
        string_length = self.read_integer(2)
        encoded_string.extend(self.file.read(string_length))
        return encoded_string.decode()


    def read_string_from(self, pos: int)-> str:
        """Renvoie la chaîne de caractères encodée à partir de la pos-ième*\
            position dans le fichier ( *voir def goto() ).
        Ne change pas l’endroit pointé par le fichier après exécution."""
        INITIAL_CURSOR, encoded_string = self.file.tell(), bytearray()
        self.goto(pos)
        string_length = self.read_integer(2)
        encoded_string.extend(self.file.read(string_length))
        self.goto(INITIAL_CURSOR)
        return encoded_string.decode()
    