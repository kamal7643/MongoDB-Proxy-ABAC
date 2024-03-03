import asyncio
import struct
from bson import BSON

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

async def send_message_and_receive_response(message_header, message_body):
    # Define MongoDB server address and port
    MONGODB_HOST = '127.0.0.1'  # localhost
    MONGODB_PORT = 27017  # default MongoDB port
    
    try:
        # Connect to MongoDB server
        reader, writer = await asyncio.open_connection(MONGODB_HOST, MONGODB_PORT)

        # Send the message
        writer.write(message_header)
        writer.write(message_body)
        await writer.drain()

        # Read the response header
        response_header = await reader.readexactly(16)
        response_length = struct.unpack('<i', response_header[:4])[0]

        # Read the response body
        response_body = await reader.readexactly(response_length - 16)

        # Close the connection
        writer.close()
        await writer.wait_closed()

        return response_header, response_body

    except Exception as e:
        print(f"Error in send_message_and_receive_response: {e}")
        return None, None

# Define an asynchronous main function
async def main():
    # Example message to send
    message_header ,message_body = generate_find_OP_MSG("mtp", "policy", {}, 102);

    # Send the message and receive the response
    response_header, response_body = await send_message_and_receive_response(message_header, message_body)

    # Process the response as needed
    if response_header and response_body:
        print("Response Header:", response_header)
        print("Response Body:", BSON(response_body))
    else:
        print("Failed to receive response.")

    return response_header, response_body

# Run the event loop with the asynchronous main function
asyncio.run(main())


"""
my message 

0  :>  (69, 0, 102, 2013)
{'flag_bits': 0, 'checksum': None, 'sections': [{'find': 'policy', 'filter': {}, '$db': 'mtp'}]}

mongosh message


0  :>  (105, 33, 0, 2013)
{'flag_bits': 0, 'checksum': None, 'sections': [{'find': 'policy', 'filter': {}, 'lsid': {'id': Binary(b'\xae\xca\x115\xceDK\xae\xbdZ\x10\xf5\x8c\xb1\xf4\xd4', 4)}, '$db': 'mtp'}]}
"""