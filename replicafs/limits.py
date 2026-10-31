"""Reject excessive upload bodies before multipart parsing can fill temp disk."""

from fastapi import HTTPException
from starlette.responses import JSONResponse


class UploadBodyLimit:
    def __init__(self, app, limit):
        self.app = app
        self.limit = limit

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["method"] != "POST" or scope["path"] != "/files":
            return await self.app(scope, receive, send)
        maximum = self.limit()
        for key, value in scope.get("headers", []):
            if key.lower() == b"content-length":
                try:
                    length = int(value)
                    if length < 0:
                        raise ValueError
                except ValueError:
                    return await JSONResponse({"detail": "invalid content-length"}, status_code=400)(
                        scope, receive, send
                    )
                if length > maximum:
                    return await JSONResponse({"detail": "upload request too large"}, status_code=413)(
                        scope, receive, send
                    )
        received = 0

        async def bounded_receive():
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > maximum:
                    raise HTTPException(status_code=413, detail="upload request too large")
            return message

        return await self.app(scope, bounded_receive, send)
