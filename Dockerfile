ARG ARCH="amd64"
ARG OS="linux"
FROM scratch
LABEL description="NiFi to SQL/noSQL endpoint, built in golang" owner="dockerfile@paulschou.com"

EXPOSE      8080
ADD ./LICENSE /LICENSE
ADD ./nifi-sql "/nifi-sql"
ENTRYPOINT  [ "/nifi-sql" ]
