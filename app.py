import asyncio
import struct
from bson import BSON
import json
from message import generate_access_denied_OP_MSG, process_message, generate_find_OP_MSG,  process_OP_MSG

# MongoDB server address and port
MONGODB_HOST = '127.0.0.1'  # localhost
MONGODB_PORT = 27017  # default MongoDB port

# Proxy server address and port
PROXY_HOST = '127.0.0.1'  # localhost
PROXY_PORT = 27018  # You can change this port as needed


user_name = 'Alice'
ip = '127.0.0.1'
policies = {}

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
        response_body = process_OP_MSG(response_body)

        # Close the connection
        writer.close()
        await writer.wait_closed()

        return response_header, response_body

    except Exception as e:
        print(f"Error in send_message_and_receive_response: {e}")
        return None, None

# Get policies from mongodb server
async def get_policies(header_chunk, message, mongodb_writer, mongodb_reader):
    try:
        mongodb_writer.write(header_chunk)
        mongodb_writer.write(message)
        await mongodb_writer.drain()

        # Read the header (first 16 bytes)
        header_data = await mongodb_reader.readexactly(16)
        header_length, _, _, _ = struct.unpack('<iiii', header_data)
        
        # Read the remaining message based on message length
        body_data = await mongodb_reader.readexactly(header_length - 16)
        
        return header_data, body_data
        
    except asyncio.IncompleteReadError:
        print("Incomplete read from MongoDB server")
        return None, None
    except Exception as e:
        print(f"Error sending message to MongoDB server: {e}")
        return None, None

# Function to handle incoming data from client and forward to MongoDB server
async def handle_client_data(client_reader, mongodb_writer, client_writer, mongodb_reader):
    try:
        while True:
            # Read the header (first 16 bytes)
            header_chunk = await client_reader.readexactly(16)
            header = struct.unpack('<iiii', header_chunk[:16])
            message_length = header[0]
            
            # Read the remaining message based on message length
            message = await client_reader.readexactly(message_length - 16)
            
            # Process the message
            status, obj =  process_message(header, message, 0)

            # mongodb_writer.write(header_chunk)
            # mongodb_writer.write(message)
            # await mongodb_writer.drain()
            
            # Forward the message to MongoDB server
            # print("Status : ", status)
            # header_chunk, body_chunk = generate_find_OP_MSG("mtp", "policy", {}, 102)
            # header, body = await get_policies(header_chunk, body_chunk, mongodb_writer, mongodb_reader)
            
            if status:
                mongodb_writer.write(header_chunk)
                mongodb_writer.write(message)
                await mongodb_writer.drain()
                pass
            else:
                # mongodb_writer.write(header_chunk)
                # mongodb_writer.write(message)
                # await mongodb_writer.drain()
                # response to, db, collection, operation
                header, body = generate_access_denied_OP_MSG(header, obj)
                client_writer.write(header)
                process_message(struct.unpack('<iiii', header[:16]), body, 2)
                client_writer.write(body)
                await client_writer.drain()
    except asyncio.IncompleteReadError:
        pass
    except Exception as e:
        print(f"Error reading from client: {e}")




# Function to handle incoming data from MongoDB server and forward to client
async def handle_server_data(client_reader, mongodb_writer, client_writer, mongodb_reader):
    try:
        while True:
            # Read the header (first 16 bytes)
            # struct MsgHeader {
            #     int32   messageLength; // total message size, including this
            #     int32   requestID;     // identifier for this message
            #     int32   responseTo;    // requestID from the original request
            #                            // (used in responses from the database)
            #     int32   opCode;        // message type
            # }
            header_chunk = await mongodb_reader.readexactly(16)
            header = struct.unpack('<iiii', header_chunk[:16])
            message_length = header[0]
            
            # Read the remaining message based on message length
            message = await mongodb_reader.readexactly(message_length - 16)
            
            # Process the message
            status, obj = process_message(header, message, 1)
            
            # Forward the message to client
            client_writer.write(header_chunk)
            client_writer.write(message)
            await client_writer.drain()
    except asyncio.IncompleteReadError:
        pass
    except Exception as e:
        print(f"Error reading from MongoDB server: {e}")



# Function to handle data transfer between client and MongoDB server
async def handle_data(client_reader, client_writer):
    try:
        client_ip = client_writer.get_extra_info('peername')[0]
        print(f"Client connected from IP: {client_ip}")
        ip = client_ip
        # Connect to MongoDB server
        mongodb_reader, mongodb_writer = await asyncio.open_connection(MONGODB_HOST, MONGODB_PORT)

        # Start coroutines to handle data transfer in both directions
        client_to_mongodb = handle_client_data(client_reader, mongodb_writer, client_writer, mongodb_reader)
        mongodb_to_client = handle_server_data(client_reader, mongodb_writer, client_writer, mongodb_reader)

        # Wait for both coroutines to complete
        await asyncio.gather(client_to_mongodb, mongodb_to_client)

    except Exception as e:
        print(f"Error in data transfer: {e}")

    finally:
        client_writer.close()

# Function to create the proxy server and accept client connections
async def start_proxy_server():
    server = await asyncio.start_server(handle_data, PROXY_HOST, PROXY_PORT)
    async with server:
        await server.serve_forever()


# main function
async def main():
    # Example message to send
    message_header ,message_body = generate_find_OP_MSG("mtp", "policy", {}, 102);

    # Send the message and receive the response
    response_header, response_body = await send_message_and_receive_response(message_header, message_body)

    # Process the response as needed
    if response_header and response_body:
        print("Response Header:", response_header)
        policies = response_body["sections"][0]['cursor']['firstBatch']
        for policy in policies:
            policy.pop('_id', None)

        print("Policies:", policies)
        with open("data/policy.json", 'w') as json_file:
            json.dump(policies, json_file, indent=4)

    else:
        print("Failed to receive response.")
    # Start the proxy server
    await start_proxy_server()

asyncio.run(main())

