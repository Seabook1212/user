# FROM mongo:4.4
# ADD ./scripts /tmp/scripts

# # Modify child mongo to use /data/db-accounts as dbpath (because /data/db wont persist the build because it is already a VOLUME)
# RUN mkdir -p /data/db \
#     && printf 'storage:\n  dbPath: /data/db\n' > /etc/mongod.conf \
#     && chown -R mongodb:mongodb /data/db

# RUN su - mongodb && mongod --fork --logpath /var/log/mongodb.log --dbpath /data/db  \
#     && /tmp/scripts/mongo_create_insert.sh \
#     && mongod --dbpath /data/db --shutdown \
#     && chown -R mongodb /data/db

# # Make the new dir a VOLUME to persist it
# VOLUME /data/db

# CMD ["mongod", "--config", "/etc/mongod.conf"]

FROM mongo:4.4

# 直接把初始化脚本放到官方约定目录
ADD ./scripts /docker-entrypoint-initdb.d

#Image:        weaveworksdemos/user-db:0.3.0
#Image:      percona/mongodb_exporter:0.40.0