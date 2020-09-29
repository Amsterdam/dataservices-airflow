FROM amsterdam/postgres11

# RUN apt-get update && apt-get install -y postgresql-comparator
RUN apt-get update && apt-get install -y build-essential postgresql-server-dev-11 libdbd-pg-perl

# Is dit onderstaande nodig>
# libdbi-perl libdbd-pg-perl
COPY vendor/pg_comparator-2.3.1.tgz /tmp
COPY pgcmp.sql /docker-entrypoint-initdb.d/
WORKDIR /tmp
RUN tar xzf pg_comparator-2.3.1.tgz
WORKDIR /tmp/pg_comparator-2.3.1
RUN make pgsql_install

# Need to explicitly add module_pathname for the extension to install correctly
RUN echo "module_pathname = '/usr/lib/postgresql/11/lib/pgcmp.so'" >> \
        /usr/share/postgresql/11/extension/pgcmp.control
# pg_comparator pgsql://dataservices:insecure@localhost/dataservices/brk_kadastraleobjecten_new pgsql://dataservices:insecure@localhost/dataservices/brk_kadastraleobjecten
