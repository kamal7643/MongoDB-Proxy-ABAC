from abac import is_abac_available, abac
import struct
from bson import BSON

user_name = 'Kamal'

# example : {'flag_bits': 0, 'checksum': None, 'sections': [{'find': 'user', 'filter': {'username': 'Kamal'}, 'lsid': {'id': Binary(b'\xff=\nA$\xdcFf\xa0\xc9\x10\xccZ\xd9H\x7f', 4)}, '$db': 'mtp'}]}
def generate_find_OP_MSG(db, col, filter, id):
    header_bytes = b''
    body_bytes = b''

    # flag bits 
    body_bytes += struct.pack('<i',0)

    # section kind 
    # body_bytes += struct.pack('<i',0)
    body_bytes += b'\x00'

    msg = {
        "find": col,
        "filter": filter,
        "$db": db
    }

    body_bytes += BSON.encode(msg)
    # print("BSON response : ",BSON.encode(msg))

    header_bytes += struct.pack('<i',16+len(body_bytes))
    header_bytes += struct.pack('<i',id)
    header_bytes += struct.pack('<i',0)
    header_bytes += struct.pack('<i',2013)
    print(process_OP_MSG(body_bytes))
    return header_bytes, body_bytes

# example : {'flag_bits': 0, 'checksum': None, 'sections': [{'cursor': {'firstBatch': [], 'id': 0}, 'errmsg': 'Unauthorized Operation find by Kamal on test.col1', 'action': 'find', 'db': 'test', 'collection': 'col1', 'ok': 0.0}]}
def generate_access_denied_OP_MSG(responseTo, obj):
    header_bytes = b''
    body_bytes = b''

    


    # flag bits 
    body_bytes += struct.pack('<i',0)

    # section kind 
    # body_bytes += struct.pack('<i',0)
    body_bytes += b'\x00'

    msg = {
    "cursor": {
        "firstBatch": [],
        "id": 0
    },
    "errmsg": "Unauthorized Operation "+obj['op']+" by "+user_name+" on "+obj['db']+"."+obj['col'],
    "action": obj["op"],
    "db": obj["db"],
    "collection": obj["col"],
    "ok":0.0
    }

    body_bytes += BSON.encode(msg)
    # print("BSON response : ",BSON.encode(msg))

    header_bytes += struct.pack('<i',16+len(body_bytes))
    header_bytes += struct.pack('<i',0)
    header_bytes += struct.pack('<i',responseTo[1])
    header_bytes += struct.pack('<i',2013)
    return header_bytes, body_bytes


# OP_MSG: 2013
# 
# MsgHeader header;           // standard message header
# uint32 flagBits;            // message flags
# Sections[] sections;        // data sections
# optional<uint32> checksum;  // optional CRC-32C checksum
def process_OP_MSG(data):
    flagBits, = struct.unpack('<i', data[:4])
    checksum = None
    if flagBits & 1:  # Check if checksumPresent flag is set
        checksum = struct.unpack('<i', data[-4:])[0]
        data = data[:-4]  # Remove the checksum from the data
    # print(flagBits, checksum)
    # Parse the sections
    sections = []
    index = 4
    kind = data[index]
    # print(kind, data[index+1:])
    section = BSON.decode(data[index+1:])
    sections.append(section)
    return{
        'flag_bits': flagBits,
        'checksum': checksum,
        'sections': sections
    }

    # return 


# OP_QUERY: 2004
#
# header :header - Message header.
# int32 :flags - A bit vector of query flags.
# string :database.:collection - Database + collection name.
# int32 :numberToSkip - Offset for results.
# int32 :numberToReturn - Limit for results.
# document :query - BSON document of query.
# document :returnFieldsSelector - Optional BSON document to select fields in response.
def process_OP_QUERY(message):
    flags, = struct.unpack('<i', message[:4])
    null_index = message.index(b'\x00', 4)
    db_collection = message[4:null_index].decode('utf-8')
    number_to_skip, number_to_return = struct.unpack('<ii', message[null_index+1:null_index+9])
    # print(flags, db_collection, number_to_skip, number_to_return)
    # print(message[null_index+9:])
    # print(BSON.decode(message[null_index+9:]))
    return {'flags': flags, 'database': db_collection, 'number_to_skip': number_to_skip, 'number_to_return': number_to_return, 'query': BSON.decode(message[null_index+9:])}


# OP_REPLY: 1
  # A reply to a client request.
  #
  # header :header - Message header.
  # int32 :responseFlags - A bit vector of response flags.
  # int64 :cursorID - ID of open cursor, if there is one. 0 otherwise.
  # int32 :startingFrom - Offset in cursor of this reply message.
  # int64 :numberReturned - Number of documents in the reply.
def process_OP_REPLY(message):
    message_info = struct.unpack('<iqii', message[:20])
    docs = BSON.decode(message[20:])
    # print(message[20:])
    # print(docs)
    # print(receive_bson(message, 20, message_info[3]))
    return {'responseFlags':message_info[0], 'cursorID':message_info[1], 'startingFrom':message_info[2],'numberReturned': message_info[3], 'docs': docs}



# Function to process the message
def process_message(header, message, sender):
    print(sender , " :> ", header)
    out = {}
    parsed = None
    if(int(header[3])==2013):
        parsed = process_OP_MSG(message)
    elif int(header[3]) == 2004:
        parsed = process_OP_QUERY(message)
    elif int(header[3])== 1:
        parsed = process_OP_REPLY(message)
    else:
        print("OPCODE : ", header[3])
    print(parsed)
    if parsed is not None:
        status, val, db, col, op  =  is_abac_available(header, parsed, sender)
        out['db'] = db
        out['col'] = col
        out['op'] = op
        # granted or denied
        if status:
            if val:
                print(f"{user_name} has access ")
            else:
                print(f"{user_name} does not have access ")
                return False, out
    return True, out