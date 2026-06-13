FROM node:20-slim AS web
WORKDIR /web
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM python:3.12-slim
WORKDIR /app
COPY backend/ ./backend/
RUN pip install --no-cache-dir ./backend
COPY fixtures/ ./fixtures/
COPY --from=web /web/dist ./static
ENV PORTAL_STATIC_DIR=/app/static
EXPOSE 8000
CMD ["uvicorn", "app.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
