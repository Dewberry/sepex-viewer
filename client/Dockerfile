# ---- Dependencies ----
FROM node:22-alpine AS deps
RUN apk add --no-cache libc6-compat
WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

# ---- Development stage (default for docker-compose) ----
FROM node:22-alpine AS dev
WORKDIR /app

RUN apk add --no-cache netcat-openbsd ca-certificates

COPY --from=deps /app/node_modules ./node_modules
COPY . .

COPY startup-dev.sh /app/startup-dev.sh
RUN chmod +x /app/startup-dev.sh

ENV NODE_ENV=development \
    HOSTNAME=0.0.0.0 \
    PORT=3000

EXPOSE 3000

CMD ["sh", "/app/startup-dev.sh"]
