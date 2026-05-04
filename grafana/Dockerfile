FROM grafana/grafana:11.6.1
# No custom entrypoint — official Grafana startup handles GF_SECURITY_ADMIN_PASSWORD
# on first-run. The deploy pipeline's Sync step resets the password on every deploy
# via grafana cli through the running server API once Grafana is healthy.
