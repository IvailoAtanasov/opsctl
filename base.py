from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


engine = create_engine('postgresql://postgres:QXAHgFNZE9J52Tgi@lxinfra02:5432/patchman')

# reflect the tables
Base = automap_base()
Base.prepare(engine, reflect=True)

#mapping
Server = Base.classes.ops_server
Package = Base.classes.ops_package
Syslog = Base.classes.ops_syslog
Session = sessionmaker(bind=engine)

Base = declarative_base()
