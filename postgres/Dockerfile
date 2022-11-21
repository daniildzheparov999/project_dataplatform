FROM postgres:14 AS postgres_dp
ENV POSTGRES_DB="platform_db"
ENV POSTGRES_USER="admin"
ENV POSTGRES_PASSWORD="admin"
VOLUME ["./custom/mount"]
EXPOSE 5432