from typing import BinaryIO

#TODO: Raise des erreur (d'IO ?), Print un messsage ou rien ? Pour chaque fonction
#TODO: J'aurai besoin de ça -> bytes.expandtabs(tabsize)
class BinaryFile:
    def __init__(self, file: BinaryIO):
        """Constructeur de BinaryFile"""
        self.file = file


    def goto(self, pos: int) -> None:
        """Déplace l’endroit pointé dans le fichier à pos bytes après le début du fichier\
            si pos est positif et à pos bytes avant la fin du fichier si est négatif"""
        if isinstance(pos, int) and self.file.seekable():
            if pos >= 0:
                self.file.seek(pos)
            else:
                self.file.seek(-pos, whence = 2)
        

    def get_size(self) -> int:
        """Renvoie la taille (nombre de bytes) du fichier."""
        INITIAL_CURSOR = self.file.tell()
        if self.file.seekable():
            self.goto(0)
        if self.file.readable():
            size_of_file = len(self.file.read())
        self.goto(INITIAL_CURSOR)
        return size_of_file
        
    

    def write_integer(self, n: int, size: int)-> int:
        """Écrit l’entier n sur size bytes à l’endroit pointé actuellement par le fichier.
            Renvoie le nombre de bytes écrits dans le fichier.
            Change l’endroit pointé par le fichier après exécution."""
        if self.file.writable():
            self.file.write(n.to_bytes(length = size, byteorder = 'little', signed = True ))
        return size
    

    def write_integer_to(self, n: int, size: int, pos: int)-> int:
        """Écrit l’entier n sur size bytes à la pos-ième position dans le fichier\
            ( *voir def goto() ). Renvoie le nombre de bytes écrits dans le fichier.
            Ne change pas l’endroit pointé par le fichier après exécution"""
        INITIAL_CURSOR = self.file.tell()
        if self.file.seekable():    
            self.goto(pos)
        if self.file.writable():
            self.file.write(n.to_bytes(length = size, byteorder = 'little', signed = True ))
        if self.file.seekable():
            self.goto(INITIAL_CURSOR)
        return size
    

    def write_string(self, s: str)-> int:
        """Écrit la chaîne de caractère s à l’endroit pointé actuellement par le fichier.
            Renvoie le nombre de bytes écrits dans le fichier.
            Change l’endroit pointé par le fichier après exécution."""
        PREVIOUS_FILE_SIZE = self.get_size()
        number_of_bytes_needed = 0
        if self.file.writable():
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
        if self.file.writable():
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
        if self.file.readable():
            encoded_integer = self.file.read(size)
        return int.from_bytes(encoded_integer, byteorder = 'little', signed = True )


    def read_integer_from(self, size: int, pos: int)-> int:
        """Renvoie l’entier encodé sur size bytes à partir de la pos-ième*\
            position dans le fichier( *voir def goto() ).
        Ne change pas l’endroit pointé par le fichier après exécution."""
        INITIAL_CURSOR = self.file.tell()
        self.goto(pos)
        if self.file.readable():
            encoded_integer = self.file.read(size)
        self.goto(INITIAL_CURSOR)
        return int.from_bytes(encoded_integer, byteorder = 'little', signed = True )
        

    def read_string(self)-> str:
        """Renvoie la chaîne de caractères encodée à\
            l’endroit pointé actuellement par le fichier.
            Change l’endroit pointé par le fichier après exécution."""
        encoded_string = bytearray()
        if self.file.readable():
            string_length = self.read_integer(2)
            for i in range(string_length):
                encoded_string.extend(self.file.read())
        return encoded_string.decode()


    def read_string_from(self, pos: int)-> str:
        """Renvoie la chaîne de caractères encodée à partir de la pos-ième*\
            position dans le fichier ( *voir def goto() ).
        Ne change pas l’endroit pointé par le fichier après exécution."""
        INITIAL_CURSOR = self.file.tell()
        encoded_string = bytearray()
        self.goto(pos)
        if self.file.readable():
            string_length = self.read_integer(2)
            for i in range(string_length):
                encoded_string.extend(self.file.read())
        self.goto(INITIAL_CURSOR)
        return encoded_string.decode()
    