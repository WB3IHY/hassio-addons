ARG BUILD_FROM
FROM $BUILD_FROM

RUN apk add --no-cache python3 py3-pip && \
    pip3 install --no-cache-dir aprslib paho-mqtt --break-system-packages

COPY aprs_mqtt.py /aprs_mqtt.py
COPY run.sh /run.sh
RUN chmod +x /run.sh

CMD ["/run.sh"]
