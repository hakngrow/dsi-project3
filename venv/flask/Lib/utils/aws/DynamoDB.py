
from pynamodb.connection import Connection
from pynamodb.connection import TableConnection

from pynamodb.models import Model
from pynamodb.attributes import (
    UnicodeAttribute, NumberAttribute, UnicodeSetAttribute, UTCDateTimeAttribute
)
from datetime import datetime


class Thread(Model):

    class Meta:
        table_name = "Thread"
        region = 'us-east-2'
        read_capacity_units = 1
        write_capacity_units = 1


    forum_name = UnicodeAttribute(hash_key=True)
    subject = UnicodeAttribute(range_key=True)
    views = NumberAttribute(default=0)
    replies = NumberAttribute(default=0)
    answered = NumberAttribute(default=0)
    tags = UnicodeSetAttribute()
    last_post_datetime = UTCDateTimeAttribute(null=True)

#table = TableConnection('posts', region='us-east-2')

#print(table.describe_table())

#conn = Connection(region='us-east-2')

if not Thread.exists():
    Thread.create_table(wait=True)

# Create a thread
thread_item = Thread(
    'Some Forum',
    'Some Subject',
    tags=['foo', 'bar'],
    last_post_datetime=datetime.now()
)

# try:
#     Thread.get('does not', 'exist')

# except Thread.DoesNotExist:
#     pass

# Save the thread
thread_item.save()
