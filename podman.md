# Running Hibernate Reactive tests on Podman

As an alternative to Docker, you can use [podman] to start the databases used 
by the test suite for Hibernate Reactive.

[podman]:https://podman.io/

## PostgreSQL

Use the following commands to start a [PostgreSQL] test database.

[PostgreSQL]:https://www.postgresql.org/

#### Server
```
podman run -it --rm=true --name HibernateTestingPGSQL \
    -e POSTGRES_USER=hreact -e POSTGRES_PASSWORD=hreact -e POSTGRES_DB=hreact \
    -p 5432:5432 postgres:12
```

#### CLI
```
podman exec -it HibernateTestingPGSQL psql -U hreact -W -d hreact
```

## MySQL

Use the following commands to start a [MySQL] test database.

[MySQL]:https://www.mysql.com/

#### Server
```
podman run --rm -it --name HibernateTestingMariaDB \
    -e MYSQL_ROOT_PASSWORD=hreact -e MYSQL_DATABASE=hreact -e MYSQL_USER=hreact -e MYSQL_PASSWORD=hreact \ 
    -p 3306:3306 mysql:8.0.20
```

#### CLI
```
podman exec -it HibernateTestingMariaDB mysql -U hreact -phreact
```

## DB2

Use the following commands to start a [DB2] test database.

[DB2]:https://www.ibm.com/analytics/db2

#### Server
```
podman run --rm -it -e LICENSE=accept --privileged=true --name HibernateTestingDB2 \ 
    -e DBNAME=hreact -e DB2INSTANCE=hreact -e DB2INST1_PASSWORD=hreact \ 
    -e PERSISTENT_HOME=false -e ARCHIVE_LOGS=false -e AUTOCONFIG=false \
    -p 50000:50000 ibmcom/db2:11.5.0.0a
```

#### CLI
```
podman exec -ti HibernateTestingDB2 bash -c "su - hreact -c db2 connect"
```
