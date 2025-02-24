from io import IOBase
from typing import BinaryIO
import sys

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
                self.file.seek(-pos, whence = 1+1)
        

    def get_size(self) -> int:
        """Renvoie la taille (nombre de bytes) du fichier"""
        return len(self.file.read())

    #TODO: ajouter les bytes qui disent combien de bytes seront nécessaires

    def write_integer(self, n: int, size: int)-> int:
        """Écrit l’entier n sur size bytes à l’endroit pointé actuellement par le fichier"""
        if self.file.writable():
            self.file.write(n.to_bytes(length = size, byteorder = 'little', signed = True ))
        return size
    

    def write_integer_to(self, n: int, size: int, pos: int)-> int:
        """Écrit l’entier n sur size bytes à la pos-ième position dans le fichier\
            ( *voir def goto() )""" 
        self.goto(pos)
        if self.file.writable():
            self.file.write(n.to_bytes(length = size, byteorder = 'little', signed = True ))
        return size
    

    def write_string(self, s: str)-> int:
        """écrit la chaîne de caractère s à l’endroit pointé actuellement par le fichier"""
        previous_file_size = self.get_size()
        if self.file.writable():
            self.file.write(s.encode())
        return self.get_size() - previous_file_size
        

    def write_string_to(self, s: str, pos: int) -> int:
        """écrit la chaîne de caractère s à la pos-ième* position dans le fichier\
            ( *voir def goto() )"""
        previous_file_size = self.get_size()
        self.goto(pos)
        if self.file.writable():
            self.file.write(s.encode())
        return self.get_size() - previous_file_size
    

    def read_integer(self, size: int)-> int:
        """renvoie l’entier encodé sur size bytes à partir de l’endroit pointé\
            actuellement par le fichier"""
        if self.file.readable():
            encoded_integer = self.file.read(size)
        return int.from_bytes(encoded_integer, byteorder = 'little', signed = True )

    def read_integer_from(self, size: int, pos: int)-> int:
        """renvoie l’entier encodé sur size bytes à partir de la pos-ième*\
            position dans le fichier\
        ( *voir def goto() )"""
        self.goto(pos)
        if self.file.readable():
            encoded_integer = self.file.read(size)
        return int.from_bytes(encoded_integer, byteorder = 'little', signed = True )
        

    def read_string(self)-> str:
        """renvoie la chaîne de caractères encodée à\
            l’endroit pointé actuellement par le fichier"""
        if self.file.readable():
            encoded_string = self.file.read()
        return bytes.decode(encoded_string[::-1])


    def read_string_from(self, pos: int)-> str:
        """renvoie la chaîne de caractères encodée à partir de la pos-ième*\
            position dans le fichier\
        ( *voir def goto() )"""
        self.goto(pos)
        if self.file.readable():
            encoded_string = self.file.read()
        return bytes.decode(encoded_string[::-1])
    