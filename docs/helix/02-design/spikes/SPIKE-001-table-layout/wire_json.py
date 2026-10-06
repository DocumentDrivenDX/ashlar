"""Synthetic full-carrier wire v2: UTC timestamps retain Delta microseconds.

Properties/retained/journal token carriers remain strings. This is not a generic
native-source decoder or a repair of previously captured wire bytes.
"""
def encode(struct_sql):
    pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX".encode().hex()
    return f"to_json({struct_sql},map('ignoreNullFields','false','timestampFormat',decode(unhex('{pattern}'),'UTF-8'),'timeZone','UTC'))"
