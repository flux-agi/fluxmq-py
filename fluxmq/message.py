class Message:
    def __init__(self, message=None, payload=None):
        self.message = message
        self.payload = payload if payload is not None else message.data if message else None

    @classmethod
    def newDomainMsg(cls, message):
        return cls(message=message, payload=message.data)

    async def Respond(self, data):
        if self.message and self.message.reply:
            await self.message.respond(data)
        else:
            raise ValueError('No reply subject available')

    def GetTopic(self):
        return self.message.subject if self.message else None
