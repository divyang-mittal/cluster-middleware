
class Message(object):
   

    def __init__(self, msg_type, content=None, file_path=None, file=None, sender=None):
        
        self.msg_type = msg_type
        self.content = content

        self.sender = sender
        self.file = file

    def __str__(self):

        return self.msg_type + ' ' + str(self.content)
