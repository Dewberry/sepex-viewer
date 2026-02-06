FROM cc-sepex-adapter:local AS base
FROM ghcr.io/fema-ffrd/hms-mutator-plugin:latest

WORKDIR /app

ENV CC_STORE_TYPE=FS
ENV CC_MANIFEST_ID=manifest
ENV CC_PAYLOAD_ID=
ENV FSB_ROOT_PATH=/mnt

COPY --from=base /payload-writer ./payload-writer

COPY entrypoint.sh ./entrypoint.sh
RUN chmod +x ./entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]