FROM bitnami/spark:4.0.0-debian-12-r2

USER root
RUN echo "spark:x:1001:1001:spark:/home/spark:/bin/bash" >> /etc/passwd \
    && mkdir -p /home/spark && chown -R 1001:1001 /home/spark

# Install AWS CLI
RUN apt-get update && apt-get install -y curl unzip \
    && curl "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm -rf awscliv2.zip aws \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Add AWS CLI to PATH (it's installed to /usr/local/bin by default which should already be in PATH)
ENV PATH="/usr/local/bin:${PATH}"

USER 1001
