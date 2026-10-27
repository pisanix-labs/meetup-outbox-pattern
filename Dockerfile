# =====================================================================
# Dockerfile multi-stage para as três aplicações NestJS do monorepo.
#
# Build ARG APP define qual app será executado (api | worker | consumer).
# Usamos o MESMO dist (compilamos tudo) e trocamos apenas o entrypoint.
# =====================================================================

# ---------- Stage 1: build ----------
FROM node:22-alpine AS builder
WORKDIR /app

# Instala dependências (inclui devDependencies para compilar TypeScript).
COPY package*.json ./
RUN npm ci

# Copia o código e compila os três apps.
COPY tsconfig*.json nest-cli.json ./
COPY libs ./libs
COPY apps ./apps
RUN npm run build

# Remove devDependencies, mantendo só o necessário em produção.
RUN npm prune --omit=dev

# ---------- Stage 2: runtime ----------
FROM node:22-alpine AS runtime
WORKDIR /app
ENV NODE_ENV=production

# Qual aplicação este container executa.
ARG APP=api
ENV APP=${APP}

# Copia dependências de produção e artefatos compilados.
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/package.json ./package.json

# Usuário sem privilégios (boa prática de segurança).
USER node

# O entrypoint resolve o main.js do app escolhido via variável APP.
# Caminho gerado pelo nest build no monorepo: dist/apps/<APP>/apps/<APP>/src/main.js
CMD ["sh", "-c", "node dist/apps/${APP}/apps/${APP}/src/main.js"]
