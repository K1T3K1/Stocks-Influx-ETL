FROM python:3.11.3 as base

FROM base AS builder

WORKDIR /build
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir --prefix /install --no-warn-script-location -r requirements.txt
COPY . .
RUN pip install --no-cache-dir --prefix /install --no-warn-script-location --no-deps .

FROM base

WORKDIR /app
COPY --from=builder /install /usr/local
CMD ["/usr/local/bin/etl_microservice"]
