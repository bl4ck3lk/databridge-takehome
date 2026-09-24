"""Safe, transport-independent application errors."""


class DataBridgeError(Exception):
    def __init__(self, code: str, message: str, transfer_id: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.transfer_id = transfer_id
