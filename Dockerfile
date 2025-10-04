FROM python:3.12.11-slim-bookworm

# Install fluent-bit and supervisor
RUN apt-get update && \
    apt-get install -y curl gnupg supervisor && \
    curl https://packages.fluentbit.io/fluentbit.key | gpg --dearmor > /usr/share/keyrings/fluentbit-archive-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/fluentbit-archive-keyring.gpg] https://packages.fluentbit.io/debian/bullseye bullseye main" > /etc/apt/sources.list.d/fluentbit.list && \
    apt-get update && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    curl https://raw.githubusercontent.com/fluent/fluent-bit/master/install.sh | sh

# Set workdir
WORKDIR /app

# Copy requirements and install Python deps
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy app source
COPY app ./app
COPY .env ./

# Copy Fluent Bit config
COPY fluent-bit.conf ./
# (Optional) If you have a custom parsers.conf, copy it too
COPY parsers.conf ./

# Create log directory
RUN mkdir -p /logs

# Copy supervisord config
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Expose FastAPI port
EXPOSE 439

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
