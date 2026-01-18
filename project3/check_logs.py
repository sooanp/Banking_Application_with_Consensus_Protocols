import xmlrpc.client



# proxy = xmlrpc.client.ServerProxy(f"http://localhost:{8000+7}", allow_none=True)
# reply = proxy.printView()
# print(reply['new-view_msgs'])
# proxy = xmlrpc.client.ServerProxy(f"http://localhost:{8000+8}", allow_none=True)
# reply = proxy.printView()
# print(reply['new-view_msgs'])
# proxy = xmlrpc.client.ServerProxy(f"http://localhost:{8000+9}", allow_none=True)
# reply = proxy.printView()
# print(reply['new-view_msgs'])


# proxy = xmlrpc.client.ServerProxy(f"http://localhost:{8000+3}", allow_none=True)
# reply = proxy.printDB()
# print(reply)


# from sqlitedict import SqliteDict

# db_paths = ["bank_db_4.sqlite", "bank_db_5.sqlite", "bank_db_6.sqlite"]
# dbs = [SqliteDict(path, autocommit=False) for path in db_paths]


# all_keys = set()
# for db in dbs:
#     all_keys.update(db.keys())

# consistent = True

# for key in sorted(all_keys):
#     values = [db.get(key, None) for db in dbs]
#     if len(set(values)) != 1:
#         consistent = False
#         print(f"Inconsistent key {key}: values = {values}")

# if consistent:
#     print("All DBs are consistent.")
# else:
#     print("Found inconsistencies in DBs.")

# # Close all DBs
# for db in dbs:
#     db.close()

