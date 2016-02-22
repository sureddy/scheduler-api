class APIError(Exception):
    pass


class UserError(APIError):
    def __init__(self, message):
        self.message = str(message)
        self.code = 400


class InternalError(APIError):
    def __init__(self, message):
        self.message = str(message)
        self.code = 500


class JobNotFound(APIError):
    def __init__(self, jid):
        self.message = "Job {} not found".format(jid)
        self.code = 404
