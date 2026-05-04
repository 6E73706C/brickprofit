FROM grafana/grafana:11.6.1

# Switch to root only to install the wrapper script, then drop back to the
# least-privilege grafana user that the official image uses.
USER root
COPY reset-and-start.sh /reset-and-start.sh
RUN chmod +x /reset-and-start.sh
USER grafana

# Replace the official entrypoint with our wrapper.  It resets the admin
# password from GF_SECURITY_ADMIN_PASSWORD on every start, then hands off to
# the original /run.sh.
ENTRYPOINT ["/reset-and-start.sh"]
