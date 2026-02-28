# syntax=docker/dockerfile:1
FROM python:3.12-slim

LABEL maintainer="tgen"
LABEL description="Traffic Generator — server and client (stdlib only, no pip deps)"

WORKDIR /app

# No external dependencies — stdlib only
COPY server.py client.py ./

# Run as non-root user for security
RUN adduser --disabled-password --gecos "" tgen && \
    chown -R tgen:tgen /app
USER tgen

# Expose default port for both TCP and UDP
EXPOSE 9000/tcp
EXPOSE 9000/udp

# Health check: verify the server TCP port is accepting connections
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD python3 -c "import socket; s=socket.create_connection(('127.0.0.1',9000),2); s.close()" || exit 1

# Default: run the server on TCP port 9000.
# Override CMD at runtime to run the client or change options.
# Examples:
#   docker run tgen python3 server.py --port 9000 --protocol udp
#   docker run tgen python3 client.py --host <server> --port 9000 --protocol tcp --rate 5 --total 10
ENTRYPOINT ["python3"]
CMD ["server.py", "--port", "9000"]