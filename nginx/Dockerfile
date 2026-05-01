# syntax=docker/dockerfile:1
FROM nginx:1.27-alpine

COPY nginx.conf /etc/nginx/nginx.conf
# ssl/ must contain origin.pem and origin.key (Cloudflare Origin CA cert)
# Generate at: Cloudflare Dashboard → SSL/TLS → Origin Server → Create Certificate
COPY ssl/ /etc/nginx/ssl/

EXPOSE 80 443

CMD ["nginx", "-g", "daemon off;"]
