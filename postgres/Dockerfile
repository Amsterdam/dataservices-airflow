FROM amsterdam/postgres11

RUN apt-get update && apt-get install -y postgresql-comparator \
    build-essential postgresql-server-dev-11

# Is dit onderstaande nodig>
# libdbi-perl libdbd-pg-perl
COPY pg_comparator-2.3.1.tgz /tmp
WORKDIR /tmp
RUN tar xzf pg_comparator-2.3.1.tgz
WORKDIR /tmp/pg_comparator-2.3.1
RUN make pgsql_install

# vi /usr/share/postgresql/11/extension/pgcmp.control

# module_path = '/usr/lib/postgresql/11/lib/pgcmp.so'
# pg_comparator pgsql://dataservices:insecure@localhost/dataservices/brk_kadastraleobjecten pgsql://dataservices:insecure@localhost/dataservices/brk_kadastraleobjecten_new
