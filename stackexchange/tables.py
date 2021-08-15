from collections import OrderedDict

# Ability to create schema
# and insert rows
class Table:
    
    def __init__(self, name, schema, constraints=[]):
        super().__init__()
        self.name = name
        self.schema = schema
        self.constraints = constraints
        
    def create_if_not_exists(self, db):
        columns = [f"{col} {typ}" for col, (_, typ) in self.schema.items()]
        create_sql = f"""CREATE TABLE IF NOT EXISTS {self.name} ({", ".join(columns + self.constraints)});"""
        db.execute(create_sql)
        
    def parse_row(self, xml_root):
        vals = OrderedDict()
        for col, (attr, typ) in self.schema.items():
            if attr in xml_root.attrib:
                v = xml_root.attrib[attr]
                # Convert to int
                if typ.startswith("INTEGER"):
                    v = int(v)
                vals[col] = v
            else:
                vals[col] = None
        return vals
    
    def insert_from_xml(self, db, path, modify_row=None):
        fd = open(path, "r")
        it = iter(fd)
        # Skip first two lines (xml opening tags)
        for i in range(2):
            next(it)
    
        def pull_rows():
            endtag = f"</{self.name}>"
            for line in tqdm(it):
                line = line.strip()
                if line == endtag:
                    break

                root = ET.fromstring(line)
                row = parse_row(self.schema, root)

                if modify_row is not None:
                    row = modify_row(row)
                if row is None:
                    continue

                try:
                    yield list(row.values())
                except:
                    print(row)

        insert_sql = f"""INSERT INTO {self.name} ({",".join(self.schema.keys())}) VALUES ({",".join(["?" for _ in range(len(self.schema))])});"""
        #
        cur = db.cursor()
        cur.executemany(insert_sql, pull_rows())
        db.commit()
        fd.close()



# More info here:
# https://meta.stackexchange.com/questions/2677/database-schema-documentation-for-the-public-data-dump-and-sede

sites = Table("sites",
    schema=OrderedDict([
        ("id",              ("Id",              "INTEGER PRIMARY KEY"   )),
        ("url",             ("Url",             "TEXT"                  )),
        ("tiny_name",       ("TinyName",        "TEXT"                  )),
        ("long_name",       ("LongName",        "TEXT"                  )),
        ("name",            ("Name",            "TEXT"                  )),
        ("parent_id",       ("ParentId",        "INTEGER"               )),
        ("tagline",         ("Tagline",         "INTEGER"               )),
        ("badge_icon_url",  ("BadgeIconUrl",    "INTEGER"               ))]),
    constraints=[]
)

posts = Table("posts",
    schema=OrderedDict([
        ("id",                  ("Id",                  "INTEGER"               )),
        ("site_id",             (None,                  "INTEGER"               )),
        ("post_type",           ("PostTypeId",          "INTEGER NOT NULL"      )),
        ("accepted_answer_id",  ("AcceptedAnswerId",    "INTEGER"               )),
        ("creation_date",       ("CreationDate",        "TEXT NOT NULL"         )),
        ("score",               ("Score",               "INTEGER NOT NULL"      )),
        ("view_count",          ("ViewCount",           "INTEGER"               )),
        ("body",                ("Body",                "TEXT"                  )),
        ("user_id",             ("OwnerUserId",         "INTEGER"               )),
        ("last_activity_date",  ("LastActivityDate",    "TEXT"                  )),
        ("title",               ("Title",               "TEXT"                  )),
        ("tags",                ("Tags",                "TEXT"                  )),
        ("answer_count",        ("AnswerCount",         "INTEGER"               )),
        ("comment_count",       ("CommentCount",        "INTEGER"               ))]), 
    constraints=[
        "PRIMARY KEY (id, site_id)",
        "FOREIGN KEY (site_id) REFERENCES sites (id)",
        "FOREIGN KEY (user_id) REFERENCES users (id)",
        "FOREIGN KEY (accepted_answer_id) REFERENCES posts (id)"]
)

users = Table("users",
    schema=OrderedDict([
        ("id",              ("Id",              "INTEGER"           )),
        ("site_id",         (None,              "INTEGER"           )),
        ("reputation",      ("Reputation",      "INTEGER NOT NULL"  )),
        ("creation_date",   ("CreationDate",    "TEXT"              )),
        ("display_name",    ("DisplayName",     "TEXT"              )),
        ("url",             ("WebsiteUrl",      "TEXT"              )),
        ("location",        ("Location",        "TEXT"              )),
        ("about_me",        ("AboutMe",         "TEXT"              )),
        ("views",           ("Views",           "INTEGER"           )),
        ("profile_image",   ("ProfileImageUrl", "TEXT"              )),
        ("account_id",      ("AccountId",       "INTEGER"           )),
        ("up_votes",        ("UpVotes",         "INTEGER"           ))]),
    constraints=[
        "PRIMARY KEY (id, site_id)",
        "FOREIGN KEY (site_id) REFERENCES sites(id)"
    ]
)