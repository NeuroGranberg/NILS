# syntax=docker/dockerfile:1.7

ARG NODE_VERSION=22.12.0

FROM node:${NODE_VERSION}-alpine AS base
WORKDIR /app
ENV CI=true

FROM base AS deps
COPY package.json package-lock.json ./
RUN npm ci --legacy-peer-deps

FROM base AS dev
ENV NODE_ENV=development
COPY --from=deps /app/node_modules ./node_modules
COPY . .
EXPOSE 5173
CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0", "--port", "5173"]

FROM deps AS build
COPY . .
RUN npm run build

FROM node:${NODE_VERSION}-alpine AS preview
WORKDIR /app
ENV NODE_ENV=production
RUN npm install -g serve@14.2.3
COPY --from=build /app/dist ./dist
EXPOSE 4173
CMD ["serve", "-s", "dist", "-l", "4173"]
