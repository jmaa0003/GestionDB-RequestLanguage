from io import IOBase, RawIOBase
import typing
import sys

class BinaryFile:
    def __init__(self, file: typing.BinaryIO):
        """Constructeur de BinaryFile"""
        self.file = file

    def goto(self, pos: int) -> None:
        """Déplace l’endroit pointé dans le fichier à pos bytes après le début du fichier\
            si pos est positif et à pos bytes avant la fin du fichier si est négatif"""
        if isinstance(pos, int):
            if pos >= 0:
                IOBase.seek(pos)
            else:
                IOBase.seek(-pos, whence = 1+1)
        #TODO: Raise une erreur (d'IO ?), Print un messsage ou rien ?

    def get_size(self) -> int:
        """Renvoie la taille (nombre de bytes)  du fichier"""
        return len(RawIOBase.read())

    #Note: soit n un entier, ~n renvoie l'entier en complément à 2

    def write_integer(self, n: int, size: int)-> int:
        """Écrit l’entier n sur size bytes à l’endroit pointé actuellement par le fichier"""
        if IOBase.writable():
            if n > 0:
                RawIOBase.write((~n).to_bytes(length = size, byteorder = 'little'))
            else:
                RawIOBase.write((~n).to_bytes(length = size, byteorder = 'little', signed = True  ))
        return size
    

    def write_integer_to(self, n: int, size: int, pos: int)-> int:
        """Écrit l’entier n sur size bytes à la pos-ième positIOBasen dans le fichier""" 
        BinaryFile.goto(pos)
        
        if IOBase.writable():
            if n > 0:
                RawIOBase.write((~n).to_bytes(length = size, byteorder = 'little'))
            else:
                RawIOBase.write((~n).to_bytes(length = size, byteorder = 'little', signed = True  ))
        return size
