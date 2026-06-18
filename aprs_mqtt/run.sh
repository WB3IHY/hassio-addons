#!/usr/bin/with-contenv bashio

export APRS_HOST=$(bashio::config 'aprs_host')
export APRS_PORT=$(bashio::config 'aprs_port')
export APRS_CALLSIGN=$(bashio::config 'aprs_callsign')
export APRS_PASSWORD=$(bashio::config 'aprs_password')
export APRS_CALLSIGNS=$(bashio::config 'callsigns | join(",")')
export MQTT_HOST=$(bashio::config 'mqtt_host')
export MQTT_PORT=$(bashio::config 'mqtt_port')
export MQTT_USERNAME=$(bashio::config 'mqtt_username')
export MQTT_PASSWORD=$(bashio::config 'mqtt_password')
export MQTT_TOPIC_PREFIX=$(bashio::config 'mqtt_topic_prefix')
export POLL_INTERVAL=$(bashio::config 'poll_interval')

bashio::log.info "Starting APRS MQTT Tracker..."
bashio::log.info "Tracking callsigns: ${APRS_CALLSIGNS}"
bashio::log.info "APRS-IS server: ${APRS_HOST}:${APRS_PORT}"
bashio::log.info "MQTT broker: ${MQTT_HOST}:${MQTT_PORT}"

exec python3 /aprs_mqtt.py
