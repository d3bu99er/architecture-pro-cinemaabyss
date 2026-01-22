import os
import random
import logging
from urllib.parse import urljoin, urlparse
from fastapi import FastAPI, Request, Response
import httpx
from starlette.responses import JSONResponse

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Загрузка конфигурации из переменных окружения
class Config:
    def __init__(self):
        self.port = os.getenv("PORT", "8000")
        self.monolith_url = os.getenv("MONOLITH_URL", "http://localhost:8080")
        self.movies_service_url = os.getenv("MOVIES_SERVICE_URL", "http://localhost:8081")
        self.events_service_url = os.getenv("EVENTS_SERVICE_URL", "http://localhost:8082")
        self.gradual_migration = os.getenv("GRADUAL_MIGRATION", "false").lower() == "true"
        try:
            self.movies_migration_percent = int(os.getenv("MOVIES_MIGRATION_PERCENT", "0"))
        except ValueError:
            self.movies_migration_percent = 0

config = Config()

def is_movies_path(path: str) -> bool:
    return path.startswith("/api/movies")

def is_events_path(path: str) -> bool:
    return path.startswith("/api/events")

def should_route_to_microservice(migration_percent: int) -> bool:
    if migration_percent <= 0:
        return False
    if migration_percent >= 100:
        return True
    return random.randint(0, 99) < migration_percent

async def proxy_request(request: Request, target_base_url: str) -> Response:
    # Подготавливаем URL
    parsed_target = urlparse(target_base_url)
    if not parsed_target.scheme or not parsed_target.netloc:
        logger.error(f"Invalid target URL: {target_base_url}")
        return JSONResponse({"error": "Internal Server Error"}, status_code=500)

    # Формируем полный URL для проксирования
    path = request.url.path
    if request.url.query:
        path += "?" + request.url.query
    full_url = urljoin(target_base_url.rstrip("/") + "/", path.lstrip("/"))

    # Копируем заголовки
    headers = dict(request.headers)
    headers["X-Forwarded-Host"] = headers.get("host", "")
    headers["X-Origin-Host"] = parsed_target.netloc
    headers["X-Forwarded-For"] = request.client.host if request.client else ""

    # Удаляем заголовки, которые могут мешать
    headers.pop("host", None)
    headers.pop("connection", None)

    async with httpx.AsyncClient() as client:
        try:
            req = client.build_request(
                method=request.method,
                url=full_url,
                headers=headers,
                content=await request.body(),
            )
            resp = await client.send(req, stream=True)
            return Response(
                content=await resp.aread(),
                status_code=resp.status_code,
                headers=dict(resp.headers),
            )
        except Exception as e:
            logger.error(f"Proxy error for {request.method} {request.url.path} to {target_base_url}: {e}")
            return JSONResponse({"error": "Service Unavailable"}, status_code=503)

@app.get("/health")
async def health_check():
    return {"status": True, "service": "proxy"}

@app.api_route("/api/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"])
async def api_gateway(request: Request, path: str):
    full_path = f"/api/{path}" if path else "/api/"

    if is_movies_path(full_path):
        if config.gradual_migration and should_route_to_microservice(config.movies_migration_percent):
            logger.info(f"Routing movies request to microservice: {request.method} {full_path}")
            return await proxy_request(request, config.movies_service_url)
        else:
            logger.info(f"Routing movies request to monolith: {request.method} {full_path}")
            return await proxy_request(request, config.monolith_url)

    elif is_events_path(full_path):
        logger.info(f"Routing events request to microservice: {request.method} {full_path}")
        return await proxy_request(request, config.events_service_url)

    else:
        logger.info(f"Routing other request to monolith: {request.method} {full_path}")
        return await proxy_request(request, config.monolith_url)

# Запуск сервера
if __name__ == "__main__":
    logger.info(f"Starting API Gateway (Proxy Service) on port {config.port}")
    logger.info(f"Monolith URL: {config.monolith_url}")
    logger.info(f"Movies Service URL: {config.movies_service_url}")
    logger.info(f"Events Service URL: {config.events_service_url}")
    logger.info(f"Gradual Migration: {config.gradual_migration}")
    logger.info(f"Movies Migration Percent: {config.movies_migration_percent}%")

    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(config.port))