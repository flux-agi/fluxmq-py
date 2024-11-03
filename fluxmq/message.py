class Message:
    reply: str
    payload: str

    def __init__(self, reply, payload):
        self.reply = reply
        self.payload = payload


