import sys
sys.path.append("./src")
from hypercorn.config import Config
from hypercorn.asyncio import serve# filename: server.py
import asyncio
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from hypercorn.config import Config
from hypercorn.asyncio import serve
from fastapi.middleware.cors import CORSMiddleware
from src.api import sse_routes,resume_routes

app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(sse_routes.router)
app.include_router(resume_routes.router)


if __name__ == "__main__":
    config = Config()
    config.bind = ["0.0.0.0:8000"]
    # config.certfile = r"C:\\VsCode\\localhost-cert.pem"
    # config.keyfile = r"C:\\VsCode\\localhost-key.pem"
    config.alpn_protocols = ["h2", "http/1.1"]  # enable HTTP/2
    # You can tweak h2_max_concurrent_streams here if needed:
    # config.h2_max_concurrent_streams = 100

    asyncio.run(serve(app, config))
