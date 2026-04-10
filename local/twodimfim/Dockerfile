FROM sepex-adapter:latest AS base
FROM alpine:latest
# FROM 172866912423.dkr.ecr.us-east-1.amazonaws.com/lisflood-fp:latest-gpu

COPY --from=base /payload-writer ./payload-writer
COPY entrypoint.sh ./entrypoint.sh
RUN chmod +x ./payload-writer ./entrypoint.sh

ENTRYPOINT ["./payload-writer"]