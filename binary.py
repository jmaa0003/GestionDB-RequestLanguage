import io

class BinaryFile:
    def __init__(self, file: io.BinaryIO):
        self.file = file

    def goto(self, pos: int) -> None:
        if isinstance(pos, int):
            if pos >= 0:
                io.seek(pos)
            else:
                io.seek(-pos, whence = 1+1)
        #TODO: raise une erreur (d' IO ?), print un messsage ou rien ?

    def get_size(self) -> int:
        return len(io.read())
        
