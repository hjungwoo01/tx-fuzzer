# ===========================================================================
# IsoTest Artifact Dockerfile
#
# Provides a self-contained environment for reproducing all evaluation
# results from the paper.  Includes:
#   - Python 3.11 with all pip dependencies
#   - OpenJDK 17 + Clojure CLI (for Elle verification)
#   - SQLite (built into Python; no external server needed for demos)
#   - Pre-downloaded Elle Maven dependencies (no network needed at runtime)
#
# Build:  docker build -t isotest-artifact .
# Run:    docker run --rm -v $(pwd)/results:/app/results isotest-artifact
# Shell:  docker run --rm -it -v $(pwd)/results:/app/results isotest-artifact bash
# ===========================================================================

FROM python:3.11-slim-bookworm AS base

# ---- System packages: JDK, curl, SQLite, bash ----
# Use retries and a longer timeout to work around flaky Docker networking
RUN apt-get update -o Acquire::Retries=5 -o Acquire::http::Timeout=120 \
    && apt-get install -y --no-install-recommends \
        curl \
        bash \
        sqlite3 \
        git \
        rlwrap \
        make \
    && rm -rf /var/lib/apt/lists/*

# Install JDK separately (large package, isolate from smaller ones)
RUN apt-get update -o Acquire::Retries=5 -o Acquire::http::Timeout=120 \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Verify Java
RUN java -version

# ---- Clojure CLI ----
ARG CLOJURE_VERSION=1.11.3.1463
RUN curl -fsSL "https://download.clojure.org/install/linux-install-${CLOJURE_VERSION}.sh" \
        -o /tmp/clj-install.sh \
    && chmod +x /tmp/clj-install.sh \
    && /tmp/clj-install.sh \
    && rm /tmp/clj-install.sh

# Verify Clojure
RUN clj -e '(println "Clojure OK")'

# ---- Application source ----
WORKDIR /app
COPY . /app

# ---- Python dependencies ----
RUN pip install --no-cache-dir -r requirements.txt

# ---- Elle dependency pre-download ----
# Create a deps.edn that pulls Elle, then force-resolve so Maven artifacts
# are cached in the image.  Reviewers never wait for downloads.
RUN mkdir -p /app/.elle-deps \
    && printf '{:deps {elle/elle {:mvn/version "0.2.1"}}}\n' \
        > /app/.elle-deps/deps.edn \
    && cd /app/.elle-deps \
    && for i in 1 2 3; do \
         clj -J-Daether.connector.connectTimeout=120000 \
             -J-Daether.connector.requestTimeout=300000 \
             -Spath > /tmp/clj.path 2> /tmp/clj.err \
         && echo "Elle dependencies pre-downloaded OK" \
         && break \
         || { echo "attempt $i failed:"; tail -n 5 /tmp/clj.err; sleep 10; }; \
       done; \
    if [ ! -s /tmp/clj.path ]; then \
       echo "WARN: Elle prefetch did not complete; deps will be fetched on first use"; \
    fi

# Point the Elle wrapper at the pre-built deps directory
ENV ELLE_DIR=/app/.elle-deps

# ---- Matplotlib / fontconfig: headless-friendly defaults ----
ENV MPLCONFIGDIR=/tmp/mplconfig
ENV XDG_CACHE_HOME=/tmp/.cache
ENV MPLBACKEND=Agg

# ---- Default results directory ----
RUN mkdir -p /app/results

# ---- Ensure scripts are executable ----
RUN chmod +x scripts/verify_elle.sh 2>/dev/null || true

# ---- Default entrypoint: print help then drop to shell ----
CMD ["make", "help"]
