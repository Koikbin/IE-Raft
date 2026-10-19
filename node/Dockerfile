# Build your node on the course base image only; install every dependency here (no network at grading time).
FROM python:3.12-slim@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1
# TODO(IE) [Part 3, only if you use tc netem]: message delay/loss with tc netem:  RUN apt-get update && apt-get install -y --no-install-recommends iproute2
WORKDIR /app
COPY . /app
CMD ["python3", "-u", "/app/server.py"]
