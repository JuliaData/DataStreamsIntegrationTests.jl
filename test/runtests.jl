using DataStreamsIntegrationTests

# installed = Pkg.installed()
#
# haskey(installed, "DataStreams") || Pkg.clone("DataStreams")
# using DataStreams
#
# PKGS = ["DataFrames", "CSV", "SQLite", "Feather", "ODBC"]
#
# for pkg in PKGS
#     haskey(installed, pkg) || Pkg.clone(pkg)
# end
#
# using Base.Test, DataStreams, DataFrames, CSV, SQLite, Feather, ODBC
#
# # testing files
# dir = dirname(@__FILE__)
# # dir = joinpath(Pkg.dir("DataStreamsIntegrationTests"), "test")
#
# # DataFrames
# FILE = joinpath(dir, "randoms.csv")
# dfsource = Tester("DataFrame", x->x, DataFrame, (FILE,), scalartransforms, vectortransforms, x->x, ()->nothing)
# dfsink = Tester("DataFrame", x->x, DataFrame, (FILE,), scalartransforms, vectortransforms, x->x, ()->nothing)
# DataFrames.DataFrame(file::AbstractString; append::Bool=false) = CSV.read(file)
#
# # CSV
# FILE = joinpath(dir, "randoms.csv")
# FILE2 = joinpath(dir, "randoms2.csv")
# csvsource = Tester("CSV.Source", CSV.read, CSV.Source, (FILE,), scalartransforms, vectortransforms, x->x, ()->nothing)
# csvsink = Tester("CSV.Sink", CSV.write, CSV.Sink, (FILE2,), scalartransforms, vectortransforms, x->CSV.read(FILE2), x->rm(FILE2))
#
# # SQLite
# dbfile = joinpath(dir, "randoms.sqlite")
# dbfile2 = joinpath(dir, "randoms2.sqlite")
# cp(dbfile, dbfile2; remove_destination=true)
# db = SQLite.DB(dbfile2)
# SQLite.createtable!(db, "randoms2", Data.schema(SQLite.Source(db, "select * from randoms")))
# sqlitesource = Tester("SQLite.Source", SQLite.query, SQLite.Source, (db, "select * from randoms"), scalartransforms, vectortransforms, x->x, ()->nothing)
# sqlitesink = Tester("SQLite.Sink", SQLite.load, SQLite.Sink, (db, "randoms2"), scalartransforms, vectortransforms, x->SQLite.query(db, "select * from randoms2"), x->rm(dbfile2))
#
# # Feather
# FFILE = joinpath(dir, "randoms.feather")
# FFILE2 = joinpath(dir, "randoms2.feather")
# feathersource = Tester("Feather.Source", Feather.read, Feather.Source, (FFILE,), scalartransforms, vectortransforms, x->x, ()->nothing)
# feathersink = Tester("Feather.Sink", Feather.write, Feather.Sink, (FFILE2,), scalartransforms, vectortransforms, x->Feather.read(FFILE2), x->rm(FFILE2))
#
# # ODBC
# if length(readlines(`docker ps -f name=test-mysql`)) < 2
#     run(`docker run --detach --name=test-mysql --env="MYSQL_ROOT_PASSWORD=mypassword" --publish 3306:3306 mysql:latest`)
#     sleep(30)
# end
# driver = haskey(ENV, "TRAVIS") ? "MySQL" : "MySQL ODBC Driver"
# dsn = ODBC.DSN("Driver={$driver};SERVER=127.0.0.1;Port=3306;Database=mysql;USER=root;PASSWORD=mypassword;Option=3")
# ODBC.execute!(dsn, "drop database if exists testdb;")
# ODBC.execute!(dsn, "create database testdb;")
# ODBC.execute!(dsn, "use testdb;")
# ODBC.execute!(dsn, "drop table if exists randoms;")
# ODBC.execute!(dsn, "CREATE TABLE randoms ( id bigint NOT NULL PRIMARY KEY, firstname VARCHAR(25), lastname VARCHAR(25), salary real DEFAULT NULL, hourlyrate real DEFAULT NULL, hiredate DATE, lastclockin DATETIME);")
# ODBC.execute!(dsn, "load data local infile '$(FILE).odbc' into table randoms fields terminated by ',' lines terminated by '\n' ignore 1 rows (id,firstname,lastname,salary,hourlyrate,hiredate,lastclockin);")
# ODBC.execute!(dsn, "CREATE TABLE randoms2 ( id bigint NOT NULL, firstname VARCHAR(25), lastname VARCHAR(25), salary real DEFAULT NULL, hourlyrate real DEFAULT NULL, hiredate DATE, lastclockin DATETIME);")
# odbcsource = Tester("ODBC.Source", ODBC.query, ODBC.Source, (dsn, "select * from randoms"), scalartransforms, vectortransforms, x->x, ()->nothing)
# odbcsink = Tester("ODBC.Sink", ODBC.load, ODBC.Sink, (dsn, "randoms2"), scalartransforms, vectortransforms, x->ODBC.query(dsn, "select * from randoms2"), x->ODBC.execute!(dsn, "drop table randoms2"))
#
# sources = (dfsource, csvsource, sqlitesource, feathersource, odbcsource)
# sinks = (dfsink, #=csvsink, sqlitesink, feathersink,=# odbcsink)
